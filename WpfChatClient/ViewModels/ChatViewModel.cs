using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media.Imaging;
using System.Windows.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using CommunityToolkit.Mvvm.Messaging;
using WpfChatClient.Core.Models;
using WpfChatClient.Models;
using WpfChatClient.Core.Interfaces;
using WpfChatClient.Infrastructure;
using WpfChatClient.Messages;
using WpfChatClient.Services;
using Microsoft.Win32;

namespace WpfChatClient.ViewModels;

public partial class ChatViewModel : ObservableObject, IDisposable, IRecipient<ConnectionSuccessMessage>
{
    private readonly IChatService _chatService;
    private readonly MessageCache _messageCache;
    private readonly IStickerService _stickerService;
    private readonly IFileTransferService _fileTransferService;
    private bool _disposed;
    private DateTime _lastTypingSent = DateTime.MinValue;
    private readonly DispatcherTimer _typingClearTimer;
    private readonly HashSet<string> _typingUsers = new();
    private readonly Dictionary<string, User> _knownUsers = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, RoomItem> _roomLookup = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<ChatMessage>> _roomMessages = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, HashSet<string>> _roomMessageIds = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, FileTransferItem> _fileTransfers = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, CancellationTokenSource> _fileTransferCancellations = new(StringComparer.OrdinalIgnoreCase);
    private const int PreviewMaxBytes = 200 * 1024;
    private const int PreviewDecodePixelWidth = 240;
    private static readonly HashSet<string> PreviewExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".bmp"
    };
    private bool _hasJoinedRooms;
    private bool _hasConnectedOnce;

    [ObservableProperty]
    private string _messageText = "";

    [ObservableProperty]
    private string _typingStatus = "";

    [ObservableProperty]
    private bool _isSomeoneTyping;

    [ObservableProperty]
    private string _onlineHeader = "ONLINE - 0";

    [ObservableProperty]
    private UserProfileCard? _selectedProfile;

    [ObservableProperty]
    private bool _isProfileCardOpen;

    [ObservableProperty]
    private RoomItem? _selectedRoom;

    [ObservableProperty]
    private string _currentRoomId = "General";

    [ObservableProperty]
    private bool _isConnected;

    [ObservableProperty]
    private bool _isStickerPickerOpen;

    public ObservableCollection<ChatMessage> Messages { get; } = new();
    public ObservableCollection<User> Users { get; } = new();
    public ObservableCollection<ToastNotification> Toasts { get; } = new();
    public ObservableCollection<RoomItem> Rooms { get; } = new();
    public ObservableCollection<StickerItem> Stickers { get; } = new();

    public ChatViewModel(
        IChatService chatService,
        MessageCache messageCache,
        IStickerService stickerService,
        IFileTransferService fileTransferService)
    {
        _chatService = chatService;
        _messageCache = messageCache;
        _stickerService = stickerService;
        _fileTransferService = fileTransferService;
        foreach (var sticker in _stickerService.GetBuiltInStickers())
        {
            Stickers.Add(sticker);
        }

        _chatService.MessageReceived += OnMessageReceived;
        _chatService.PrivateMessageReceived += OnPrivateMessageReceived;
        _chatService.UsersUpdated += OnUsersUpdated;
        _chatService.UserTyping += OnUserTyping;
        _chatService.ConnectionLost += OnConnectionLost;
        _chatService.ConnectionRestored += OnConnectionRestored;
        _chatService.FileOfferReceived += OnFileOfferReceived;
        _chatService.FileAvailableReceived += OnFileAvailableReceived;
        _chatService.FileTransferFailedReceived += OnFileTransferFailedReceived;

        IsConnected = _chatService.IsConnected;
        if (IsConnected)
        {
            _hasConnectedOnce = true;
            var dispatcher = Application.Current?.Dispatcher;
            if (dispatcher == null)
            {
                _ = InitializeAfterConnectAsync();
            }
            else
            {
                dispatcher.BeginInvoke(() => _ = InitializeAfterConnectAsync());
            }
        }

        InitializeRooms();
        SelectedRoom = Rooms.FirstOrDefault(r => r.Id.Equals(CurrentRoomId, StringComparison.OrdinalIgnoreCase)) ?? Rooms.FirstOrDefault();

        WeakReferenceMessenger.Default.Register(this);

        _typingClearTimer = new DispatcherTimer
        {
            Interval = TimeSpan.FromSeconds(3)
        };
        _typingClearTimer.Tick += (s, e) =>
        {
            _typingUsers.Clear();
            UpdateTypingStatus();
            _typingClearTimer.Stop();
        };

        _ = LoadRoomHistoryAsync(CurrentRoomId);
    }

    private async Task LoadRoomHistoryAsync(string roomId)
    {
        try
        {
            var normalizedRoomId = NormalizeRoomId(roomId);
            var history = await _messageCache.GetRoomMessagesAsync(normalizedRoomId);

            var dispatcher = Application.Current?.Dispatcher;
            if (dispatcher == null) return;

            await dispatcher.InvokeAsync(() =>
            {
                var historyMessages = new List<ChatMessage>();

                foreach (var msg in history)
                {
                    DateTime time;
                    if (!DateTime.TryParse(msg.Timestamp, out time)) time = DateTime.Now;

                    historyMessages.Add(CreateMessage(msg.Username, time.ToString("HH:mm"), msg.Content, msg.MessageId));
                }

                MergeRoomHistory(normalizedRoomId, historyMessages);

                if (string.Equals(CurrentRoomId, normalizedRoomId, StringComparison.OrdinalIgnoreCase))
                {
                    ShowRoomMessages(normalizedRoomId);
                }
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CHAT] History load error: {ex.Message}");
        }
    }

    private void InitializeRooms()
    {
        Rooms.Clear();
        _roomLookup.Clear();

        var roomList = new[]
        {
            new RoomItem("General", "# general"),
            new RoomItem("Gaming", "# gaming"),
            new RoomItem("Study", "# study")
        };

        foreach (var room in roomList)
        {
            Rooms.Add(room);
            _roomLookup[room.Id] = room;
            _roomMessages.TryAdd(room.Id, new List<ChatMessage>());
            _roomMessageIds.TryAdd(room.Id, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        }
    }

    private async Task JoinAllRoomsAsync()
    {
        if (Rooms.Count == 0 || _hasJoinedRooms || !_chatService.IsConnected) return;

        foreach (var room in Rooms)
        {
            await _chatService.JoinRoomAsync(room.Id, setActive: false);
        }

        await _chatService.JoinRoomAsync(CurrentRoomId);
        _hasJoinedRooms = true;
    }

    private async Task RejoinRoomsAsync()
    {
        if (Rooms.Count == 0) return;

        await JoinAllRoomsAsync();
        await LoadRoomHistoryAsync(CurrentRoomId);
    }

    partial void OnSelectedRoomChanged(RoomItem? value)
    {
        if (value == null) return;
        _ = SwitchRoomAsync(value.Id);
    }

    private async Task SwitchRoomAsync(string roomId)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (string.Equals(CurrentRoomId, normalizedRoomId, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        CurrentRoomId = normalizedRoomId;
        _typingUsers.Clear();
        UpdateTypingStatus();
        ShowRoomMessages(normalizedRoomId);

        if (_roomLookup.TryGetValue(normalizedRoomId, out var room))
        {
            room.UnreadCount = 0;
        }

        if (_chatService.IsConnected)
        {
            await _chatService.JoinRoomAsync(normalizedRoomId);
        }

        await LoadRoomHistoryAsync(normalizedRoomId);
    }

    private ChatMessage CreateMessage(string sender, string time, string content, string messageId)
    {
        bool isOwn = string.Equals(sender, _chatService.CurrentUsername, StringComparison.Ordinal);
        var sticker = _stickerService.TryGetStickerFromMessage(content);

        if (sticker != null)
        {
            return new ChatMessage(
                sender,
                content,
                time,
                isOwn,
                messageId,
                GetAvatarColor(sender),
                IsSticker: true,
                StickerId: sticker.Id,
                StickerGlyph: sticker.Glyph,
                StickerName: sticker.DisplayName,
                StickerAccentColor: sticker.AccentColor);
        }

        return new ChatMessage(sender, content, time, isOwn, messageId, GetAvatarColor(sender));
    }

    private ChatMessage CreateFileMessage(FileTransferItem transfer)
    {
        return new ChatMessage(
            transfer.Sender,
            transfer.FileName,
            DateTime.Now.ToString("HH:mm"),
            transfer.IsOwn,
            transfer.TransferId,
            GetAvatarColor(transfer.Sender),
            IsFile: true,
            FileTransfer: transfer);
    }

    private void AddOrUpdateFileOffer(FileOfferData offer)
    {
        var normalizedRoomId = NormalizeRoomId(offer.RoomId);
        var sender = string.IsNullOrWhiteSpace(offer.Sender) ? "Unknown" : offer.Sender;
        var isOwn = string.Equals(sender, _chatService.CurrentUsername, StringComparison.Ordinal);

        if (!_fileTransfers.TryGetValue(offer.TransferId, out var transfer))
        {
            transfer = new FileTransferItem
            {
                TransferId = offer.TransferId,
                FileName = offer.FileName,
                FileSize = offer.FileSize,
                PreviewBase64 = offer.PreviewBase64,
                PreviewMime = offer.PreviewMime,
                Sender = sender,
                RoomId = normalizedRoomId,
                IsOwn = isOwn,
                Status = isOwn ? FileTransferUiStatus.Uploading : FileTransferUiStatus.Pending,
                StatusText = isOwn ? "Uploading" : "Waiting",
                Progress = isOwn ? 0 : 0
            };
            _fileTransfers[offer.TransferId] = transfer;
        }
        else
        {
            transfer.FileName = offer.FileName;
            transfer.FileSize = offer.FileSize;
            transfer.Sender = sender;
            transfer.RoomId = normalizedRoomId;
            transfer.IsOwn = isOwn;
            if (!string.IsNullOrWhiteSpace(offer.PreviewBase64))
            {
                transfer.PreviewBase64 = offer.PreviewBase64;
                transfer.PreviewMime = offer.PreviewMime;
            }
            if (transfer.Status == FileTransferUiStatus.Pending)
            {
                transfer.StatusText = "Waiting";
            }
        }

        var message = CreateFileMessage(transfer);
        var wasAdded = TryAddMessageToRoom(normalizedRoomId, message);
        if (wasAdded && string.Equals(normalizedRoomId, CurrentRoomId, StringComparison.OrdinalIgnoreCase))
        {
            Messages.Add(message);
        }
        else if (wasAdded && _roomLookup.TryGetValue(normalizedRoomId, out var room))
        {
            room.UnreadCount++;
        }
    }

    private void MarkFileAvailable(FileAvailableData file)
    {
        if (!_fileTransfers.TryGetValue(file.TransferId, out var transfer))
        {
            transfer = new FileTransferItem
            {
                TransferId = file.TransferId,
                FileName = file.FileName,
                FileSize = file.FileSize,
                Sender = "Unknown",
                RoomId = NormalizeRoomId(file.RoomId),
                IsOwn = false
            };
            _fileTransfers[file.TransferId] = transfer;
            AddOrUpdateFileOffer(new FileOfferData
            {
                TransferId = file.TransferId,
                RoomId = file.RoomId,
                Sender = transfer.Sender,
                FileName = file.FileName,
                FileSize = file.FileSize,
                CreatedAt = DateTime.Now.ToString("o")
            });
        }

        transfer.Status = FileTransferUiStatus.Available;
        transfer.Progress = 100;
        transfer.StatusText = transfer.IsOwn ? "Uploaded" : "Ready to download";
    }

    private void MarkFileFailed(FileTransferFailedData failure)
    {
        if (!_fileTransfers.TryGetValue(failure.TransferId, out var transfer))
        {
            return;
        }

        if (transfer.Status == FileTransferUiStatus.Canceled)
        {
            return;
        }

        transfer.Status = FileTransferUiStatus.Failed;
        transfer.ErrorMessage = failure.Reason;
        transfer.StatusText = "Failed";
    }

    private List<ChatMessage> GetRoomMessages(string roomId)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (!_roomMessages.TryGetValue(normalizedRoomId, out var messages))
        {
            messages = new List<ChatMessage>();
            _roomMessages[normalizedRoomId] = messages;
        }

        return messages;
    }

    private HashSet<string> GetRoomMessageIds(string roomId)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (!_roomMessageIds.TryGetValue(normalizedRoomId, out var ids))
        {
            ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _roomMessageIds[normalizedRoomId] = ids;
        }

        return ids;
    }

    private bool TryAddMessageToRoom(string roomId, ChatMessage message)
    {
        var messages = GetRoomMessages(roomId);
        var ids = GetRoomMessageIds(roomId);
        return TryAddMessage(messages, ids, message);
    }

    private static bool TryAddMessage(List<ChatMessage> messages, HashSet<string> ids, ChatMessage message)
    {
        if (!string.IsNullOrWhiteSpace(message.MessageId) && !ids.Add(message.MessageId))
        {
            return false;
        }

        messages.Add(message);
        return true;
    }

    private void MergeRoomHistory(string roomId, IReadOnlyList<ChatMessage> historyMessages)
    {
        var messages = GetRoomMessages(roomId);
        var existingMessages = messages.ToList();
        var ids = GetRoomMessageIds(roomId);

        messages.Clear();
        ids.Clear();

        foreach (var message in historyMessages)
        {
            TryAddMessage(messages, ids, message);
        }

        foreach (var message in existingMessages)
        {
            TryAddMessage(messages, ids, message);
        }
    }

    private void ShowRoomMessages(string roomId)
    {
        Messages.Clear();

        foreach (var message in GetRoomMessages(roomId))
        {
            Messages.Add(message);
        }
    }

    private static string NormalizeRoomId(string? roomId)
    {
        return string.IsNullOrWhiteSpace(roomId) ? "General" : roomId.Trim();
    }

    public void Receive(ConnectionSuccessMessage message)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null)
        {
            IsConnected = true;
            _ = InitializeAfterConnectAsync();
            return;
        }

        dispatcher.BeginInvoke(() =>
        {
            IsConnected = true;
            _hasConnectedOnce = true;
            _ = InitializeAfterConnectAsync();
        });
    }

    private async Task InitializeAfterConnectAsync()
    {
        await JoinAllRoomsAsync();
        await LoadRoomHistoryAsync(CurrentRoomId);
    }

    private void OnConnectionLost()
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            IsConnected = false;
            Users.Clear();
            _knownUsers.Clear();
            OnlineHeader = "OFFLINE";
            _typingUsers.Clear();
            UpdateTypingStatus();
            _hasJoinedRooms = false;
            AddToast("Disconnected", "Connection lost. Reconnecting...", "system", "status");
        });
    }

    private void OnConnectionRestored()
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        var isReconnect = _hasConnectedOnce;
        _hasConnectedOnce = true;

        dispatcher.BeginInvoke(() =>
        {
            IsConnected = true;
            if (isReconnect)
            {
                AddToast("Reconnected", "Connection restored.", "system", "status");
            }
            else
            {
                AddToast("Connected", "Connection established.", "system", "status");
            }
        });

        if (isReconnect)
        {
            _ = RejoinRoomsAsync();
        }
    }

    partial void OnMessageTextChanged(string value)
    {
        if (string.IsNullOrEmpty(value)) return;

        if ((DateTime.Now - _lastTypingSent).TotalSeconds > 2)
        {
            _lastTypingSent = DateTime.Now;
            _ = _chatService.SendTypingAsync(true);
        }
    }

    private void OnUserTyping(string username, bool isTyping)
    {
        if (username == _chatService.CurrentUsername) return;

        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            if (isTyping)
            {
                _typingUsers.Add(username);
                _typingClearTimer.Stop();
                _typingClearTimer.Start();
            }
            else
            {
                _typingUsers.Remove(username);
            }

            RefreshKnownUser(username);
            UpdateTypingStatus();
        });
    }

    private void UpdateTypingStatus()
    {
        IsSomeoneTyping = _typingUsers.Count > 0;
        if (!IsSomeoneTyping)
        {
            TypingStatus = "";
            return;
        }

        if (_typingUsers.Count == 1)
        {
            TypingStatus = $"{_typingUsers.First()} is typing...";
        }
        else if (_typingUsers.Count == 2)
        {
            TypingStatus = $"{string.Join(" and ", _typingUsers)} are typing...";
        }
        else
        {
            TypingStatus = "Several people are typing...";
        }
    }

    private void OnMessageReceived(string sender, string time, string content, string messageId, string roomId)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            bool isOwn = sender == _chatService.CurrentUsername;
            Console.WriteLine($"[CHAT] Message from {sender} (room: {normalizedRoomId}, isOwn: {isOwn}): {content}");
            var message = CreateMessage(sender, time, content, messageId);
            var wasAdded = TryAddMessageToRoom(normalizedRoomId, message);

            if (!wasAdded)
            {
                return;
            }

            if (string.Equals(normalizedRoomId, CurrentRoomId, StringComparison.OrdinalIgnoreCase))
            {
                Messages.Add(message);

                // If a message is received from someone typing, remove them from typing list
                if (_typingUsers.Remove(sender))
                {
                    RefreshKnownUser(sender);
                    UpdateTypingStatus();
                }
            }
            else if (_roomLookup.TryGetValue(normalizedRoomId, out var room))
            {
                room.UnreadCount++;
            }

            if (!isOwn && IsMention(content))
            {
                var roomLabel = $"#{normalizedRoomId.ToLowerInvariant()}";
                var preview = string.Equals(normalizedRoomId, CurrentRoomId, StringComparison.OrdinalIgnoreCase)
                    ? $"{sender}: {content}"
                    : $"{roomLabel} - {sender}: {content}";

                AddToast("Mention", preview, sender, "mention");
            }
        });
    }

    private void OnPrivateMessageReceived(string sender, string time, string content, string messageId)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            AddToast("Direct message", $"{sender}: {content}", sender, "dm");
        });
    }

    private void OnFileOfferReceived(FileOfferData offer)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            AddOrUpdateFileOffer(offer);
            if (_fileTransfers.TryGetValue(offer.TransferId, out var transfer) &&
                string.Equals(offer.Sender, _chatService.CurrentUsername, StringComparison.Ordinal))
            {
                TryStartUploadAfterOffer(transfer);
            }

            if (!string.Equals(offer.Sender, _chatService.CurrentUsername, StringComparison.Ordinal) &&
                !string.Equals(NormalizeRoomId(offer.RoomId), CurrentRoomId, StringComparison.OrdinalIgnoreCase))
            {
                AddToast("File shared", $"{offer.Sender}: {offer.FileName}", offer.Sender, "file");
            }
        });
    }

    private void OnFileAvailableReceived(FileAvailableData file)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            MarkFileAvailable(file);
        });
    }

    private void OnFileTransferFailedReceived(FileTransferFailedData failure)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            MarkFileFailed(failure);
            AddToast("File failed", failure.Reason, "system", "file");
        });
    }

    private void OnUsersUpdated(string[] usernames)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            Console.WriteLine($"[CHAT] Users updated: {string.Join(", ", usernames)}");
            Users.Clear();
            _knownUsers.Clear();
            foreach (var username in usernames)
            {
                var user = CreateUser(username);
                _knownUsers[username] = user;
                Users.Add(user);
            }
            OnlineHeader = $"ONLINE - {Users.Count}";

            // Remove users who left from typing list
            var currentUsers = new HashSet<string>(usernames);
            if (_typingUsers.RemoveWhere(u => !currentUsers.Contains(u)) > 0)
            {
                UpdateTypingStatus();
            }
        });
    }

    [RelayCommand]
    private async Task SendMessage()
    {
        if (string.IsNullOrWhiteSpace(MessageText)) return;

        var message = MessageText;

        try
        {
            var messageId = await _chatService.SendMessageAsync(message);
            if (messageId == null)
            {
                OnMessageReceived("SYSTEM", DateTime.Now.ToString("HH:mm"), "Message was not sent because the connection is not active.", Guid.NewGuid().ToString("N"), CurrentRoomId);
                return;
            }

            OnMessageReceived(_chatService.CurrentUsername ?? "You", DateTime.Now.ToString("HH:mm"), message, messageId, CurrentRoomId);
            MessageText = "";
            IsStickerPickerOpen = false;
            _lastTypingSent = DateTime.MinValue; // Reset so next type sends immediately
        }
        catch (System.Exception)
        {
            // Error handling
        }
    }

    [RelayCommand]
    private async Task AttachFile()
    {
        if (!_chatService.IsConnected)
        {
            AddToast("Offline", "Connect before sending a file.", "system", "file");
            return;
        }

        var dialog = new OpenFileDialog
        {
            Title = "Choose file to send",
            CheckFileExists = true,
            Multiselect = false
        };

        if (dialog.ShowDialog() != true)
        {
            return;
        }

        var fileInfo = new FileInfo(dialog.FileName);
        if (!fileInfo.Exists)
        {
            AddToast("File missing", "The selected file no longer exists.", "system", "file");
            return;
        }

        if (fileInfo.Length < 1)
        {
            AddToast("Empty file", "Choose a file with content.", "system", "file");
            return;
        }

        if (fileInfo.Length > FileTransferItem.MaxFileSizeBytes)
        {
            AddToast("File too large", "Maximum size is 10 GB.", "system", "file");
            return;
        }

        var serverIp = _chatService.ServerIp;
        if (string.IsNullOrWhiteSpace(serverIp))
        {
            AddToast("File unavailable", "Server address is not available.", "system", "file");
            return;
        }

        var username = _chatService.CurrentUsername ?? "You";
        string previewBase64 = string.Empty;
        string previewMime = string.Empty;
        if (TryCreateImagePreview(fileInfo.FullName, out var preview, out var mime))
        {
            previewBase64 = preview;
            previewMime = mime;
        }
        var transfer = new FileTransferItem
        {
            TransferId = Guid.NewGuid().ToString("N"),
            FileName = fileInfo.Name,
            FileSize = fileInfo.Length,
            PreviewBase64 = previewBase64,
            PreviewMime = previewMime,
            Sender = username,
            RoomId = CurrentRoomId,
            IsOwn = true,
            LocalPath = fileInfo.FullName,
            Status = FileTransferUiStatus.Pending,
            StatusText = "Waiting for server",
            Progress = 0
        };

        _fileTransfers[transfer.TransferId] = transfer;
        AddOrUpdateFileOffer(new FileOfferData
        {
            TransferId = transfer.TransferId,
            RoomId = transfer.RoomId,
            Sender = username,
            FileName = transfer.FileName,
            FileSize = transfer.FileSize,
            PreviewBase64 = transfer.PreviewBase64,
            PreviewMime = transfer.PreviewMime,
            CreatedAt = DateTime.Now.ToString("o")
        });

        var sent = await _chatService.SendFileOfferAsync(new FileOfferData
        {
            TransferId = transfer.TransferId,
            RoomId = transfer.RoomId,
            Sender = username,
            FileName = transfer.FileName,
            FileSize = transfer.FileSize,
            PreviewBase64 = transfer.PreviewBase64,
            PreviewMime = transfer.PreviewMime,
            CreatedAt = DateTime.Now.ToString("o")
        });

        if (!sent)
        {
            transfer.Status = FileTransferUiStatus.Failed;
            transfer.StatusText = "Failed";
            transfer.ErrorMessage = "File offer was not sent.";
            return;
        }

    }

    private static bool TryCreateImagePreview(string filePath, out string previewBase64, out string previewMime)
    {
        previewBase64 = string.Empty;
        previewMime = string.Empty;

        var extension = Path.GetExtension(filePath);
        if (string.IsNullOrWhiteSpace(extension) || !PreviewExtensions.Contains(extension))
        {
            return false;
        }

        try
        {
            using var stream = File.OpenRead(filePath);
            var bitmap = new BitmapImage();
            bitmap.BeginInit();
            bitmap.CacheOption = BitmapCacheOption.OnLoad;
            bitmap.CreateOptions = BitmapCreateOptions.IgnoreColorProfile;
            bitmap.DecodePixelWidth = PreviewDecodePixelWidth;
            bitmap.StreamSource = stream;
            bitmap.EndInit();
            bitmap.Freeze();

            BitmapEncoder encoder;
            if (extension.Equals(".png", StringComparison.OrdinalIgnoreCase))
            {
                encoder = new PngBitmapEncoder();
                previewMime = "image/png";
            }
            else
            {
                encoder = new JpegBitmapEncoder { QualityLevel = 70 };
                previewMime = "image/jpeg";
            }

            encoder.Frames.Add(BitmapFrame.Create(bitmap));

            using var output = new MemoryStream();
            encoder.Save(output);

            if (output.Length > PreviewMaxBytes)
            {
                return false;
            }

            previewBase64 = Convert.ToBase64String(output.ToArray());
            return !string.IsNullOrWhiteSpace(previewBase64);
        }
        catch
        {
            return false;
        }
    }

    private void TryStartUploadAfterOffer(FileTransferItem transfer)
    {
        if (!transfer.IsOwn ||
            string.IsNullOrWhiteSpace(transfer.LocalPath) ||
            transfer.Status is FileTransferUiStatus.Available or FileTransferUiStatus.Downloaded or FileTransferUiStatus.Failed or FileTransferUiStatus.Canceled ||
            _fileTransferCancellations.ContainsKey(transfer.TransferId))
        {
            return;
        }

        var serverIp = _chatService.ServerIp;
        if (string.IsNullOrWhiteSpace(serverIp))
        {
            transfer.Status = FileTransferUiStatus.Failed;
            transfer.StatusText = "Failed";
            transfer.ErrorMessage = "Server address is not available.";
            return;
        }

        transfer.Status = FileTransferUiStatus.Uploading;
        transfer.StatusText = "Uploading";
        _ = UploadFileInBackgroundAsync(transfer, transfer.LocalPath, serverIp, _chatService.FilePort);
    }

    private async Task UploadFileInBackgroundAsync(FileTransferItem transfer, string sourcePath, string serverIp, int filePort)
    {
        using var cts = new CancellationTokenSource();
        _fileTransferCancellations[transfer.TransferId] = cts;

        try
        {
            var progress = new Progress<FileTransferProgress>(p =>
            {
                transfer.Progress = p.Percent;
                transfer.StatusText = $"Uploading {p.Percent:0}%";
            });

            await _fileTransferService.UploadFileAsync(
                new FileUploadRequest(
                    serverIp,
                    filePort,
                    transfer.TransferId,
                    transfer.Sender,
                    transfer.RoomId,
                    sourcePath,
                    transfer.FileName,
                    transfer.FileSize),
                progress,
                cts.Token);

            transfer.Progress = 100;
            transfer.Status = FileTransferUiStatus.Uploading;
            transfer.StatusText = "Processing";
        }
        catch (OperationCanceledException)
        {
            transfer.Status = FileTransferUiStatus.Canceled;
            transfer.StatusText = "Canceled";
        }
        catch (Exception ex)
        {
            transfer.Status = FileTransferUiStatus.Failed;
            transfer.StatusText = "Failed";
            transfer.ErrorMessage = ex.Message;
            AddToast("Upload failed", ex.Message, "system", "file");
        }
        finally
        {
            _fileTransferCancellations.Remove(transfer.TransferId);
        }
    }

    [RelayCommand]
    private void DownloadFile(FileTransferItem? transfer)
    {
        if (transfer == null || !transfer.CanDownload)
        {
            return;
        }

        var serverIp = _chatService.ServerIp;
        if (string.IsNullOrWhiteSpace(serverIp))
        {
            AddToast("File unavailable", "Server address is not available.", "system", "file");
            return;
        }

        var dialog = new SaveFileDialog
        {
            Title = "Save file",
            FileName = transfer.FileName,
            OverwritePrompt = true
        };

        if (dialog.ShowDialog() != true)
        {
            return;
        }

        _ = DownloadFileInBackgroundAsync(transfer, dialog.FileName, serverIp, _chatService.FilePort);
    }

    private async Task DownloadFileInBackgroundAsync(FileTransferItem transfer, string destinationPath, string serverIp, int filePort)
    {
        using var cts = new CancellationTokenSource();
        _fileTransferCancellations[transfer.TransferId] = cts;

        try
        {
            transfer.Status = FileTransferUiStatus.Downloading;
            transfer.StatusText = "Downloading";
            transfer.Progress = 0;

            var progress = new Progress<FileTransferProgress>(p =>
            {
                transfer.Progress = p.Percent;
                transfer.StatusText = $"Downloading {p.Percent:0}%";
            });

            await _fileTransferService.DownloadFileAsync(
                new FileDownloadRequest(
                    serverIp,
                    filePort,
                    transfer.TransferId,
                    _chatService.CurrentUsername ?? string.Empty,
                    transfer.RoomId,
                    destinationPath),
                progress,
                cts.Token);

            transfer.LocalPath = destinationPath;
            transfer.Progress = 100;
            transfer.Status = FileTransferUiStatus.Downloaded;
            transfer.StatusText = "Downloaded";
        }
        catch (OperationCanceledException)
        {
            transfer.Status = FileTransferUiStatus.Available;
            transfer.StatusText = "Ready to download";
            transfer.Progress = 100;
        }
        catch (Exception ex)
        {
            transfer.Status = FileTransferUiStatus.Failed;
            transfer.StatusText = "Failed";
            transfer.ErrorMessage = ex.Message;
            AddToast("Download failed", ex.Message, "system", "file");
        }
        finally
        {
            _fileTransferCancellations.Remove(transfer.TransferId);
        }
    }

    [RelayCommand]
    private void CancelFileTransfer(FileTransferItem? transfer)
    {
        if (transfer == null)
        {
            return;
        }

        if (_fileTransferCancellations.TryGetValue(transfer.TransferId, out var cts))
        {
            transfer.StatusText = "Canceling";
            cts.Cancel();
        }
    }

    [RelayCommand]
    private void ToggleStickerPicker()
    {
        IsStickerPickerOpen = !IsStickerPickerOpen;
    }

    [RelayCommand]
    private void InsertQuickEmoji()
    {
        MessageText = string.IsNullOrWhiteSpace(MessageText)
            ? "\uD83D\uDE0A"
            : MessageText + " \uD83D\uDE0A";
    }

    [RelayCommand]
    private async Task SendSticker(string? stickerId)
    {
        if (string.IsNullOrWhiteSpace(stickerId) || !_chatService.IsConnected)
        {
            return;
        }

        try
        {
            var content = _stickerService.CreateStickerMessage(stickerId);
            var messageId = await _chatService.SendMessageAsync(content);
            if (messageId == null)
            {
                OnMessageReceived(
                    "SYSTEM",
                    DateTime.Now.ToString("HH:mm"),
                    "Sticker was not sent because the connection is not active.",
                    Guid.NewGuid().ToString("N"),
                    CurrentRoomId);
                return;
            }

            OnMessageReceived(
                _chatService.CurrentUsername ?? "You",
                DateTime.Now.ToString("HH:mm"),
                content,
                messageId,
                CurrentRoomId);
            IsStickerPickerOpen = false;
        }
        catch (ArgumentException)
        {
            OnMessageReceived(
                "SYSTEM",
                DateTime.Now.ToString("HH:mm"),
                "Unknown sticker.",
                Guid.NewGuid().ToString("N"),
                CurrentRoomId);
        }
    }

    [RelayCommand]
    private void ShowProfile(string? username)
    {
        if (string.IsNullOrWhiteSpace(username)) return;

        var profileName = username == "You" ? _chatService.CurrentUsername ?? username : username;
        var isTyping = _typingUsers.Contains(profileName);
        SelectedProfile = new UserProfileCard(
            profileName,
            GetAvatarColor(profileName),
            isTyping ? "Typing" : "Online",
            isTyping);
        IsProfileCardOpen = true;
    }

    [RelayCommand]
    private void CloseProfile()
    {
        IsProfileCardOpen = false;
        SelectedProfile = null;
    }

    [RelayCommand]
    private void DismissToast(string? toastId)
    {
        if (string.IsNullOrWhiteSpace(toastId)) return;

        var toast = Toasts.FirstOrDefault(t => t.Id == toastId);
        if (toast != null)
        {
            Toasts.Remove(toast);
        }
    }

    private void AddToast(string title, string message, string source, string kind)
    {
        var toast = new ToastNotification(
            Guid.NewGuid().ToString("N"),
            title,
            message,
            source,
            kind,
            DateTime.Now.ToString("HH:mm"));

        Toasts.Insert(0, toast);
        while (Toasts.Count > 4)
        {
            Toasts.RemoveAt(Toasts.Count - 1);
        }

        _ = DismissToastLaterAsync(toast.Id);
    }

    private async Task DismissToastLaterAsync(string toastId)
    {
        await Task.Delay(TimeSpan.FromSeconds(5));
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher != null && !dispatcher.HasShutdownStarted)
        {
            await dispatcher.InvokeAsync(() => DismissToast(toastId));
        }
    }

    private bool IsMention(string content)
    {
        var username = _chatService.CurrentUsername;
        return !string.IsNullOrWhiteSpace(username) &&
            content.Contains("@" + username, StringComparison.OrdinalIgnoreCase);
    }

    private User CreateUser(string username)
    {
        var isTyping = _typingUsers.Contains(username);
        return new User(username, GetAvatarColor(username), isTyping ? "Typing" : "Online", isTyping);
    }

    private void RefreshKnownUser(string username)
    {
        if (!_knownUsers.ContainsKey(username)) return;

        var updatedUser = CreateUser(username);
        _knownUsers[username] = updatedUser;

        var index = Users.ToList().FindIndex(u => string.Equals(u.Name, username, StringComparison.OrdinalIgnoreCase));
        if (index >= 0)
        {
            Users[index] = updatedUser;
        }
    }

    private static string GetAvatarColor(string username)
    {
        var colors = new[]
        {
            "#5865F2",
            "#43B581",
            "#F04747",
            "#FAA61A",
            "#00A8CC",
            "#9B59B6",
            "#2ECC71",
            "#E67E22"
        };

        var hash = StringComparer.OrdinalIgnoreCase.GetHashCode(username);
        var index = (int)((uint)hash % colors.Length);
        return colors[index];
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _chatService.MessageReceived -= OnMessageReceived;
            _chatService.PrivateMessageReceived -= OnPrivateMessageReceived;
            _chatService.UsersUpdated -= OnUsersUpdated;
            _chatService.UserTyping -= OnUserTyping;
            _chatService.ConnectionLost -= OnConnectionLost;
            _chatService.ConnectionRestored -= OnConnectionRestored;
            _chatService.FileOfferReceived -= OnFileOfferReceived;
            _chatService.FileAvailableReceived -= OnFileAvailableReceived;
            _chatService.FileTransferFailedReceived -= OnFileTransferFailedReceived;
            WeakReferenceMessenger.Default.Unregister<ConnectionSuccessMessage>(this);
            _typingClearTimer.Stop();
            foreach (var cts in _fileTransferCancellations.Values)
            {
                cts.Cancel();
            }

            _fileTransferCancellations.Clear();
            _disposed = true;
        }
    }
}
