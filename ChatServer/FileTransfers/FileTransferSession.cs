using System.Net.Sockets;
using System.Text.Json;

namespace ChatServer.FileTransfers;

public sealed class FileTransferSession
{
    private readonly TcpClient _client;
    private readonly FileMetadataStore _metadataStore;
    private readonly Func<string, string, string, bool> _isUserInRoomFromAddress;
    private readonly Func<FileTransferRecord, Task> _onAvailable;
    private readonly Func<FileTransferRecord, Task> _onFailed;

    public FileTransferSession(
        TcpClient client,
        FileMetadataStore metadataStore,
        Func<string, string, string, bool> isUserInRoomFromAddress,
        Func<FileTransferRecord, Task> onAvailable,
        Func<FileTransferRecord, Task> onFailed)
    {
        _client = client;
        _metadataStore = metadataStore;
        _isUserInRoomFromAddress = isUserInRoomFromAddress;
        _onAvailable = onAvailable;
        _onFailed = onFailed;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var client = _client;

        try
        {
            client.NoDelay = true;
            client.ReceiveBufferSize = FileMetadataStore.BufferSizeBytes;
            client.SendBufferSize = FileMetadataStore.BufferSizeBytes;
            await using var stream = client.GetStream();
            var remoteAddress = (client.Client.RemoteEndPoint as System.Net.IPEndPoint)?.Address.ToString() ?? string.Empty;

            var json = await FileTransferProtocol.ReadJsonLineAsync(stream, cancellationToken);
            if (string.IsNullOrWhiteSpace(json))
            {
                Console.WriteLine("[FILE] Empty file-port request.");
                return;
            }

            var request = JsonSerializer.Deserialize<FilePortRequest>(json, FileTransferProtocol.JsonOptions);
            if (request == null)
            {
                Console.WriteLine("[FILE] Invalid file-port request JSON.");
                await SendFailureAsync(stream, null, "Invalid file-port request.", cancellationToken);
                return;
            }

            if (request.Type.Equals("upload", StringComparison.OrdinalIgnoreCase))
            {
                await HandleUploadAsync(stream, request, remoteAddress, cancellationToken);
                return;
            }

            if (request.Type.Equals("download", StringComparison.OrdinalIgnoreCase))
            {
                await HandleDownloadAsync(stream, request, remoteAddress, cancellationToken);
                return;
            }

            Console.WriteLine($"[FILE] Unknown file-port request type '{request.Type}'.");
            await SendFailureAsync(stream, request.TransferId, "Unknown file-port request type.", cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FILE] Session ended after error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    private async Task HandleUploadAsync(
        NetworkStream stream,
        FilePortRequest request,
        string remoteAddress,
        CancellationToken cancellationToken)
    {
        FileTransferRecord? record = null;
        var partialPath = string.Empty;

        try
        {
            if (!_metadataStore.TryGet(request.TransferId, out record) || record == null)
            {
                Console.WriteLine($"[FILE] Upload rejected for unknown transfer '{request.TransferId}'.");
                await SendFailureAsync(stream, request.TransferId, "Transfer is not registered.", cancellationToken);
                return;
            }

            if (!string.Equals(request.Sender, record.Sender, StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(request.RoomId, record.RoomId, StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(FileMetadataStore.SanitizeFileName(request.FileName), record.SafeFileName, StringComparison.OrdinalIgnoreCase))
            {
                await SendFailureAsync(stream, record.TransferId, "Upload metadata does not match the registered offer.", cancellationToken);
                return;
            }

            if (request.FileSize != record.FileSize)
            {
                await SendFailureAsync(stream, record.TransferId, "Upload size does not match the registered offer.", cancellationToken);
                return;
            }

            if (!string.Equals(remoteAddress, record.SenderAddress, StringComparison.OrdinalIgnoreCase) ||
                !_isUserInRoomFromAddress(record.Sender, record.RoomId, remoteAddress))
            {
                await SendFailureAsync(stream, record.TransferId, "Upload is not authorized for this session.", cancellationToken);
                return;
            }

            if (!_metadataStore.TryClaimUpload(record.TransferId, out record, out var claimFailureReason))
            {
                await SendFailureAsync(stream, request.TransferId, claimFailureReason, cancellationToken);
                return;
            }

            partialPath = _metadataStore.GetPartialUploadPath(record);
            Directory.CreateDirectory(Path.GetDirectoryName(partialPath)!);

            await FileTransferProtocol.WriteJsonLineAsync(stream, new FilePortResponse
            {
                Ok = true,
                FileName = record.SafeFileName,
                FileSize = record.FileSize
            }, cancellationToken);

            Console.WriteLine($"[FILE] Upload started: {record.TransferId} ({record.SafeFileName}, {record.FileSize} bytes).");
            await ReceiveExactFileAsync(stream, partialPath, record.FileSize, cancellationToken);

            var completedDirectory = _metadataStore.GetCompletedDirectory(record);
            Directory.CreateDirectory(completedDirectory);
            var completedPath = Path.Combine(completedDirectory, record.SafeFileName);

            if (File.Exists(completedPath))
            {
                File.Delete(completedPath);
            }

            File.Move(partialPath, completedPath);

            if (_metadataStore.TryMarkAvailable(record.TransferId, completedPath, out var availableRecord) &&
                availableRecord != null)
            {
                Console.WriteLine($"[FILE] upload completed: {availableRecord.TransferId}");
                await NotifyAvailableAsync(availableRecord);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FILE] Upload failed for '{request.TransferId}': {ex.GetType().Name}: {ex.Message}");

            if (!string.IsNullOrWhiteSpace(partialPath))
            {
                TryDeleteFile(partialPath);
            }

            if (record != null &&
                _metadataStore.TryMarkFailed(record.TransferId, ex.Message, out var failedRecord) &&
                failedRecord != null)
            {
                await NotifyFailedAsync(failedRecord);
            }
        }
    }

    private async Task HandleDownloadAsync(
        NetworkStream stream,
        FilePortRequest request,
        string remoteAddress,
        CancellationToken cancellationToken)
    {
        if (!_metadataStore.TryGet(request.TransferId, out var record) || record == null)
        {
            Console.WriteLine($"[FILE] Download rejected for unknown transfer '{request.TransferId}'.");
            await SendFailureAsync(stream, request.TransferId, "Transfer is not registered.", cancellationToken);
            return;
        }

        if (record.Status != FileTransferStatus.Available)
        {
            Console.WriteLine($"[FILE] Download rejected for unavailable transfer '{record.TransferId}'.");
            await SendFailureAsync(stream, record.TransferId, "Transfer is not available.", cancellationToken);
            return;
        }

        if (!string.Equals(request.RoomId, record.RoomId, StringComparison.OrdinalIgnoreCase) ||
            !_isUserInRoomFromAddress(request.Requester, record.RoomId, remoteAddress))
        {
            Console.WriteLine($"[FILE] Download rejected because requester is not authorized for transfer '{record.TransferId}'.");
            await SendFailureAsync(stream, record.TransferId, "Download is not authorized for this session.", cancellationToken);
            return;
        }

        var completedPath = record.FinalPath ??
            Path.Combine(_metadataStore.GetCompletedDirectory(record), record.SafeFileName);

        if (!File.Exists(completedPath))
        {
            Console.WriteLine($"[FILE] Download rejected because file is missing for transfer '{record.TransferId}'.");
            await SendFailureAsync(stream, record.TransferId, "File is missing on the server.", cancellationToken);
            return;
        }

        await FileTransferProtocol.WriteJsonLineAsync(stream, new FilePortResponse
        {
            Ok = true,
            TransferId = record.TransferId,
            FileName = record.SafeFileName,
            FileSize = record.FileSize
        }, cancellationToken);

        Console.WriteLine($"[FILE] download started: {record.TransferId} by {request.Requester}");
        await using var fileStream = new FileStream(
            completedPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            FileMetadataStore.BufferSizeBytes,
            useAsync: true);

        var buffer = new byte[FileMetadataStore.BufferSizeBytes];
        int bytesRead;
        while ((bytesRead = await fileStream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken)) > 0)
        {
            await stream.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
        }

        await stream.FlushAsync(cancellationToken);
        Console.WriteLine($"[FILE] Download completed: {record.TransferId}.");
    }

    private async Task ReceiveExactFileAsync(
        NetworkStream stream,
        string partialPath,
        long expectedBytes,
        CancellationToken cancellationToken)
    {
        await using var fileStream = new FileStream(
            partialPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            FileMetadataStore.BufferSizeBytes,
            useAsync: true);

        var buffer = new byte[FileMetadataStore.BufferSizeBytes];
        var remaining = expectedBytes;
        while (remaining > 0)
        {
            var readSize = (int)Math.Min(buffer.Length, remaining);
            var bytesRead = await stream.ReadAsync(buffer.AsMemory(0, readSize), cancellationToken);
            if (bytesRead == 0)
            {
                throw new EndOfStreamException("Upload stream ended before all bytes were received.");
            }

            await fileStream.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
            remaining -= bytesRead;
        }

        await fileStream.FlushAsync(cancellationToken);
    }

    private static Task SendFailureAsync(
        NetworkStream stream,
        string? transferId,
        string error,
        CancellationToken cancellationToken)
    {
        return FileTransferProtocol.WriteJsonLineAsync(stream, new FilePortResponse
        {
            Ok = false,
            TransferId = transferId,
            Error = error
        }, cancellationToken);
    }

    private async Task NotifyAvailableAsync(FileTransferRecord record)
    {
        try
        {
            await _onAvailable(record);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FILE] Availability callback failed for '{record.TransferId}': {ex.Message}");
        }
    }

    private async Task NotifyFailedAsync(FileTransferRecord record)
    {
        try
        {
            await _onFailed(record);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FILE] Failure callback failed for '{record.TransferId}': {ex.Message}");
        }
    }

    private static void TryDeleteFile(string filePath)
    {
        try
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
