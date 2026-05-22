using System;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using WpfChatClient.Core.Models;
using WpfChatClient.Core.Interfaces;
using System.Collections.Generic;
using WpfChatClient.Infrastructure;

namespace WpfChatClient.Services;

public class ChatService : IChatService
{
    private TcpClient? _client;
    private NetworkStream? _stream;
    private StreamReader? _reader;
    private StreamWriter? _writer;
    private CancellationTokenSource? _cts;
    private CancellationTokenSource? _reconnectCts;
    private Task? _receiveTask;
    private Task? _heartbeatTask;
    private Task? _reconnectTask;
    private string? _lastIp;
    private int _lastPort;
    private string? _lastUsername;
    private volatile bool _isConnecting;
    private volatile bool _isConnected;
    private volatile bool _isReconnectLoopRunning;
    private volatile bool _isIntentionallyDisconnected = true;
    private string _activeRoomId = "General";
    private int _connectionId;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly MessageCache _messageCache;

    public event MessageReceivedHandler? MessageReceived;
    public event PrivateMessageReceivedHandler? PrivateMessageReceived;
    public event UsersUpdatedHandler? UsersUpdated;
    public event UserTypingHandler? UserTyping;
    public event ConnectionStateHandler? ConnectionLost;
    public event ConnectionStateHandler? ConnectionRestored;
    public event FileOfferReceivedHandler? FileOfferReceived;
    public event FileAvailableReceivedHandler? FileAvailableReceived;
    public event FileTransferFailedReceivedHandler? FileTransferFailedReceived;

    public string? CurrentUsername { get; private set; }
    public string? ServerIp { get; private set; }
    public int ServerPort { get; private set; }
    public int FilePort => _lastPort > 0 ? _lastPort + 1 : 5001;
    public bool IsConnected => _isConnected;

    public ChatService(MessageCache messageCache)
    {
        _messageCache = messageCache;
    }

    public async Task ConnectAsync(string ip, int port, string username)
    {
        Console.WriteLine($"[CLIENT] connect requested: {ip}:{port} as {username}");

        await _connectionLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_isConnected)
            {
                Console.WriteLine("[CLIENT] connect ignored: already connected");
                return;
            }

            if (_isConnecting)
            {
                Console.WriteLine("[CLIENT] connect ignored: already connecting");
                return;
            }

            _isConnecting = true;
            _isIntentionallyDisconnected = false;
            _lastIp = ip;
            _lastPort = port;
            _lastUsername = username;
            ServerIp = ip;
            ServerPort = port;
            CurrentUsername = username;

            await OpenConnectionLockedAsync().ConfigureAwait(false);
        }
        catch
        {
            if (!_isConnected)
            {
                ClearConnectionTarget();
                _isIntentionallyDisconnected = true;
            }

            throw;
        }
        finally
        {
            _isConnecting = false;
            _connectionLock.Release();
        }
    }

    public async Task JoinRoomAsync(string roomId, bool setActive = true)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (!_isConnected)
        {
            if (setActive) _activeRoomId = normalizedRoomId;
            return;
        }

        var sent = await TrySendPacketAsync(new Packet
        {
            Type = PacketType.RoomJoin,
            Data = JsonSerializer.SerializeToElement(new RoomJoinData { RoomId = normalizedRoomId })
        }).ConfigureAwait(false);

        if (sent && setActive) _activeRoomId = normalizedRoomId;
    }

    private async Task OpenConnectionLockedAsync()
    {
        CleanupConnectionLocked(invalidateConnection: true);

        TcpClient? client = null;
        NetworkStream? stream = null;
        StreamReader? reader = null;
        StreamWriter? writer = null;
        CancellationTokenSource? cts = null;

        try
        {
            client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(_lastIp!, _lastPort).ConfigureAwait(false);

            stream = client.GetStream();
            reader = new StreamReader(stream, Encoding.UTF8);
            writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
            cts = new CancellationTokenSource();

            await WritePacketAsync(writer, new Packet
            {
                Type = PacketType.Join,
                Data = JsonSerializer.SerializeToElement(new JoinData { Username = _lastUsername! })
            }).ConfigureAwait(false);

            await WaitForJoinAcceptedAsync(reader, cts.Token).ConfigureAwait(false);

            _client = client;
            _stream = stream;
            _reader = reader;
            _writer = writer;
            _cts = cts;
            _isConnected = true;

            ConnectionRestored?.Invoke();

            var connectionId = ++_connectionId;
            Console.WriteLine($"[CLIENT] connect success: connection #{connectionId}");

            var receiveReader = reader;
            var connectionToken = cts.Token;
            _receiveTask = Task.Run(() => ReceiveLoopAsync(connectionId, receiveReader, connectionToken));
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(connectionId, connectionToken));

            client = null;
            stream = null;
            reader = null;
            writer = null;
            cts = null;
        }
        catch
        {
            SafeDispose(cts);
            SafeDispose(reader);
            SafeDispose(writer);
            SafeDispose(stream);
            SafeDispose(client);
            _isConnected = false;
            throw;
        }
    }

    private async Task WaitForJoinAcceptedAsync(StreamReader reader, CancellationToken token)
    {
        using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        handshakeCts.CancelAfter(TimeSpan.FromSeconds(5));

        try
        {
            while (true)
            {
                var line = await reader.ReadLineAsync(handshakeCts.Token).ConfigureAwait(false);
                if (line == null)
                {
                    throw new IOException("Server closed the connection during join.");
                }

                var packet = JsonSerializer.Deserialize<Packet>(line);
                if (packet == null)
                {
                    continue;
                }

                if (packet.Type == PacketType.ConnectionRejected)
                {
                    var rejectedData = packet.Data.Deserialize<SystemMessageData>();
                    _isIntentionallyDisconnected = true;
                    throw new InvalidOperationException(rejectedData?.Message ?? "Connection rejected by server.");
                }

                ParseLine(line);

                if (IsJoinAccepted(packet))
                {
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested && handshakeCts.IsCancellationRequested)
        {
            throw new TimeoutException("Timed out waiting for join response from server.");
        }
    }

    private bool IsJoinAccepted(Packet packet)
    {
        if (packet.Type != PacketType.UserListUpdate || string.IsNullOrWhiteSpace(_lastUsername))
        {
            return false;
        }

        var userData = packet.Data.Deserialize<UserListUpdateData>();
        return userData?.Users.Exists(user => string.Equals(user, _lastUsername, StringComparison.OrdinalIgnoreCase)) == true;
    }

    public async Task<string?> SendMessageAsync(string message)
    {
        var messageId = Guid.NewGuid().ToString("N");
        var sent = await TrySendPacketAsync(new Packet
        {
            Type = PacketType.ChatMessage,
            Data = JsonSerializer.SerializeToElement(new ChatMessageData
            {
                MessageId = messageId,
                RoomId = _activeRoomId,
                Username = _lastUsername!,
                Content = message
            })
        }).ConfigureAwait(false);

        return sent ? messageId : null;
    }

    public async Task<bool> SendFileOfferAsync(FileOfferData offer)
    {
        if (offer == null)
        {
            throw new ArgumentNullException(nameof(offer));
        }

        var data = new FileOfferData
        {
            TransferId = offer.TransferId,
            FileName = offer.FileName,
            FileSize = offer.FileSize,
            Sender = string.IsNullOrWhiteSpace(offer.Sender) ? _lastUsername ?? CurrentUsername ?? string.Empty : offer.Sender,
            RoomId = string.IsNullOrWhiteSpace(offer.RoomId) ? _activeRoomId : NormalizeRoomId(offer.RoomId),
            PreviewBase64 = offer.PreviewBase64,
            PreviewMime = offer.PreviewMime,
            CreatedAt = offer.CreatedAt
        };

        return await TrySendPacketAsync(new Packet
        {
            Type = PacketType.FileOffer,
            Data = JsonSerializer.SerializeToElement(data)
        }).ConfigureAwait(false);
    }

    public async Task SendTypingAsync(bool isTyping)
    {
        await TrySendPacketAsync(new Packet
        {
            Type = PacketType.Typing,
            Data = JsonSerializer.SerializeToElement(new TypingData
            {
                Username = _lastUsername!,
                IsTyping = isTyping
            })
        }).ConfigureAwait(false);
    }

    private async Task<bool> TrySendPacketAsync(Packet packet, CancellationToken token = default)
    {
        var writer = _writer;
        var connectionId = _connectionId;
        if (writer == null || !_isConnected || token.IsCancellationRequested) return false;

        var lockTaken = false;
        try
        {
            await _writeLock.WaitAsync(token).ConfigureAwait(false);
            lockTaken = true;

            if (writer != _writer || connectionId != _connectionId || !_isConnected)
            {
                return false;
            }

            await WritePacketAsync(writer, packet).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (ObjectDisposedException)
        {
            await HandleDisconnectAsync(connectionId, "send failed: socket disposed").ConfigureAwait(false);
            return false;
        }
        catch (Exception ex)
        {
            await HandleDisconnectAsync(connectionId, $"send failed: {ex.Message}").ConfigureAwait(false);
            return false;
        }
        finally
        {
            if (lockTaken) _writeLock.Release();
        }
    }

    private static async Task WritePacketAsync(StreamWriter writer, Packet packet)
    {
        string json = JsonSerializer.Serialize(packet);
        await writer.WriteLineAsync(json).ConfigureAwait(false);
    }

    private async Task ReceiveLoopAsync(int connectionId, StreamReader reader, CancellationToken token)
    {
        Console.WriteLine($"[CLIENT] receive loop started: connection #{connectionId}");
        var disconnectReason = "remote closed connection";

        try
        {
            while (!token.IsCancellationRequested && _isConnected && connectionId == _connectionId)
            {
                string? line = await reader.ReadLineAsync(token).ConfigureAwait(false);
                if (line == null)
                {
                    break;
                }

                ParseLine(line);
            }
        }
        catch (OperationCanceledException)
        {
            disconnectReason = "cancelled";
        }
        catch (ObjectDisposedException)
        {
            disconnectReason = "socket disposed";
        }
        catch (IOException ex)
        {
            disconnectReason = $"io error: {ex.Message}";
        }
        catch (Exception ex)
        {
            disconnectReason = $"receive failed: {ex.Message}";
        }
        finally
        {
            Console.WriteLine($"[CLIENT] receive loop ended: connection #{connectionId} ({disconnectReason})");

            if (!token.IsCancellationRequested)
            {
                await HandleDisconnectAsync(connectionId, disconnectReason).ConfigureAwait(false);
            }
        }
    }

    private async Task HeartbeatLoopAsync(int connectionId, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested && _isConnected && connectionId == _connectionId)
            {
                bool sent = await TrySendPacketAsync(new Packet
                {
                    Type = PacketType.Heartbeat,
                    Data = JsonSerializer.SerializeToElement(new HeartbeatData())
                }, token).ConfigureAwait(false);
                if (!sent && !token.IsCancellationRequested)
                {
                    await HandleDisconnectAsync(connectionId, "heartbeat send failed").ConfigureAwait(false);
                    break;
                }

                await Task.Delay(5000, token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            await HandleDisconnectAsync(connectionId, $"heartbeat failed: {ex.Message}").ConfigureAwait(false);
        }
    }

    private void ParseLine(string line)
    {
        try
        {
            var packet = JsonSerializer.Deserialize<Packet>(line);
            if (packet == null) return;

            Console.WriteLine($"[CLIENT] packet received: {packet.Type}");

            switch (packet.Type)
            {
                case PacketType.ChatMessage:
                    var chatData = packet.Data.Deserialize<ChatMessageData>();
                    if (chatData != null)
                    {
                        Console.WriteLine($"[CLIENT] chat packet: room={NormalizeRoomId(chatData.RoomId)}, from={chatData.Username}, id={chatData.MessageId}");
                        _ = _messageCache.SaveMessageAsync(chatData);
                        DateTime time;
                        if (!DateTime.TryParse(chatData.Timestamp, out time)) time = DateTime.Now;
                        MessageReceived?.Invoke(chatData.Username, time.ToString("HH:mm"), chatData.Content, chatData.MessageId, NormalizeRoomId(chatData.RoomId));
                    }
                    break;
                case PacketType.PrivateMessage:
                    var privateData = packet.Data.Deserialize<PrivateMessageData>();
                    if (privateData != null)
                    {
                        _ = _messageCache.SavePrivateMessageAsync(privateData);
                        DateTime privateTime;
                        if (!DateTime.TryParse(privateData.Timestamp, out privateTime)) privateTime = DateTime.Now;
                        PrivateMessageReceived?.Invoke(privateData.Sender, privateTime.ToString("HH:mm"), privateData.Content, privateData.MessageId);
                    }
                    break;
                case PacketType.UserListUpdate:
                    var userData = packet.Data.Deserialize<UserListUpdateData>();
                    if (userData != null) UsersUpdated?.Invoke(userData.Users.ToArray());
                    break;
                case PacketType.Typing:
                    var typingData = packet.Data.Deserialize<TypingData>();
                    if (typingData != null) UserTyping?.Invoke(typingData.Username, typingData.IsTyping);
                    break;
                case PacketType.SystemMessage:
                    var systemData = packet.Data.Deserialize<SystemMessageData>();
                    if (systemData != null) MessageReceived?.Invoke("SYSTEM", DateTime.Now.ToString("HH:mm"), systemData.Message, "", _activeRoomId);
                    break;
                case PacketType.ConnectionRejected:
                    var rejectedData = packet.Data.Deserialize<SystemMessageData>();
                    Console.WriteLine($"[CLIENT] connection rejected: {rejectedData?.Message ?? "no reason"}");
                    _isIntentionallyDisconnected = true;
                    if (rejectedData != null) MessageReceived?.Invoke("SYSTEM", DateTime.Now.ToString("HH:mm"), rejectedData.Message, "", _activeRoomId);
                    break;
                case PacketType.FileOffer:
                    var fileOfferData = packet.Data.Deserialize<FileOfferData>();
                    if (fileOfferData != null) FileOfferReceived?.Invoke(fileOfferData);
                    break;
                case PacketType.FileAvailable:
                    var fileAvailableData = packet.Data.Deserialize<FileAvailableData>();
                    if (fileAvailableData != null) FileAvailableReceived?.Invoke(fileAvailableData);
                    break;
                case PacketType.FileTransferFailed:
                    var fileTransferFailedData = packet.Data.Deserialize<FileTransferFailedData>();
                    if (fileTransferFailedData != null) FileTransferFailedReceived?.Invoke(fileTransferFailedData);
                    break;
                case PacketType.Heartbeat:
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CLIENT] packet parse/dispatch failed: {ex.Message}");
        }
    }

    private async Task HandleDisconnectAsync(int connectionId, string reason)
    {
        var shouldReconnect = false;

        await _connectionLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (connectionId != _connectionId || !_isConnected)
            {
                return;
            }

            _isConnected = false;
            Console.WriteLine($"[CLIENT] disconnected: connection #{connectionId} ({reason})");
            CleanupConnectionLocked(invalidateConnection: true);
            shouldReconnect = !_isIntentionallyDisconnected;
        }
        finally
        {
            _connectionLock.Release();
        }

        ConnectionLost?.Invoke();

        if (shouldReconnect)
        {
            StartReconnectLoop();
        }
    }

    private void StartReconnectLoop()
    {
        if (_isIntentionallyDisconnected) return;

        if (_isReconnectLoopRunning)
        {
            Console.WriteLine("[CLIENT] reconnect ignored: loop already running");
            return;
        }

        _isReconnectLoopRunning = true;
        _reconnectCts?.Cancel();
        SafeDispose(_reconnectCts);
        _reconnectCts = new CancellationTokenSource();
        _reconnectTask = Task.Run(() => ReconnectLoopAsync(_reconnectCts.Token));
    }

    private async Task ReconnectLoopAsync(CancellationToken token)
    {
        Console.WriteLine("[CLIENT] reconnect started");

        try
        {
            int retryCount = 0;
            while (!_isConnected && !_isIntentionallyDisconnected && !token.IsCancellationRequested)
            {
                retryCount++;
                try
                {
                    var delay = retryCount <= 5 ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(5);
                    await Task.Delay(delay, token).ConfigureAwait(false);

                    await _connectionLock.WaitAsync(token).ConfigureAwait(false);
                    try
                    {
                        if (_isConnected || _isIntentionallyDisconnected || token.IsCancellationRequested)
                        {
                            break;
                        }

                        if (_isConnecting)
                        {
                            continue;
                        }

                        _isConnecting = true;
                        await OpenConnectionLockedAsync().ConfigureAwait(false);
                    }
                    finally
                    {
                        _isConnecting = false;
                        _connectionLock.Release();
                    }

                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CLIENT] reconnect attempt {retryCount} failed: {ex.Message}");
                }
            }
        }
        finally
        {
            _isReconnectLoopRunning = false;
            Console.WriteLine("[CLIENT] reconnect stopped");
        }
    }

    public void Disconnect()
    {
        Console.WriteLine("[CLIENT] disconnect requested");
        _isIntentionallyDisconnected = true;
        _reconnectCts?.Cancel();

        _connectionLock.Wait();
        try
        {
            _isConnecting = false;
            _isConnected = false;
            CleanupConnectionLocked(invalidateConnection: true);
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private void CleanupConnectionLocked(bool invalidateConnection)
    {
        if (invalidateConnection)
        {
            _connectionId++;
        }

        try { _cts?.Cancel(); } catch { }

        SafeDispose(_reader);
        SafeDispose(_writer);
        SafeDispose(_stream);
        SafeDispose(_client);
        SafeDispose(_cts);

        _reader = null;
        _writer = null;
        _stream = null;
        _client = null;
        _cts = null;
        _receiveTask = null;
        _heartbeatTask = null;
    }

    private void ClearConnectionTarget()
    {
        CurrentUsername = null;
        ServerIp = null;
        ServerPort = 0;
        _lastUsername = null;
        _lastIp = null;
        _lastPort = 0;
    }

    private static void SafeDispose(IDisposable? disposable)
    {
        try
        {
            disposable?.Dispose();
        }
        catch
        {
        }
    }

    private static string NormalizeRoomId(string? roomId)
    {
        return string.IsNullOrWhiteSpace(roomId) ? "General" : roomId.Trim();
    }
}
