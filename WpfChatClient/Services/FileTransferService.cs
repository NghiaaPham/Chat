using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using WpfChatClient.Core.Interfaces;

namespace WpfChatClient.Services;

public sealed class FileTransferService : IFileTransferService
{
    private const int BufferSize = 1_048_576;
    private const long MaxFileSizeBytes = 10L * 1024 * 1024 * 1024;

    public async Task UploadFileAsync(
        FileUploadRequest request,
        IProgress<FileTransferProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ValidateEndpoint(request.ServerIp, request.FilePort);
        ValidateRequired(request.TransferId, nameof(request.TransferId));
        ValidateRequired(request.FilePath, nameof(request.FilePath));

        var fileInfo = new FileInfo(request.FilePath);
        if (!fileInfo.Exists)
        {
            throw new FileNotFoundException("Source file was not found.", request.FilePath);
        }

        ValidateFileSize(fileInfo.Length);
        if (request.FileSize != fileInfo.Length)
        {
            throw new InvalidOperationException("File size changed before upload started.");
        }

        string fileName = string.IsNullOrWhiteSpace(request.FileName) ? fileInfo.Name : request.FileName;

        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(request.ServerIp, request.FilePort, cancellationToken).ConfigureAwait(false);

        NetworkStream networkStream = client.GetStream();
        await FileTransferProtocol.WriteJsonLineAsync(
            networkStream,
            new FilePortRequest
            {
                Type = "upload",
                TransferId = request.TransferId,
                Sender = request.Sender,
                RoomId = request.RoomId,
                FileName = fileName,
                FileSize = fileInfo.Length
            },
            cancellationToken).ConfigureAwait(false);

        FilePortResponse response = await ReadResponseAsync(networkStream, cancellationToken).ConfigureAwait(false);
        EnsureAccepted(response, "File upload was rejected by the server.");

        await using var fileStream = new FileStream(
            request.FilePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            BufferSize,
            useAsync: true);

        var buffer = new byte[BufferSize];
        long transferred = 0;
        ReportProgress(progress, transferred, fileInfo.Length);

        while (true)
        {
            int read = await fileStream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                break;
            }

            await networkStream.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
            transferred += read;
            ReportProgress(progress, transferred, fileInfo.Length);
        }

        await networkStream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task DownloadFileAsync(
        FileDownloadRequest request,
        IProgress<FileTransferProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ValidateEndpoint(request.ServerIp, request.FilePort);
        ValidateRequired(request.TransferId, nameof(request.TransferId));
        ValidateRequired(request.DestinationPath, nameof(request.DestinationPath));

        string partPath = request.DestinationPath + ".part";

        try
        {
            string? directory = Path.GetDirectoryName(request.DestinationPath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            using var client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(request.ServerIp, request.FilePort, cancellationToken).ConfigureAwait(false);

            NetworkStream networkStream = client.GetStream();
            await FileTransferProtocol.WriteJsonLineAsync(
                networkStream,
                new FilePortRequest
                {
                    Type = "download",
                    TransferId = request.TransferId,
                    Requester = request.Requester,
                    RoomId = request.RoomId
                },
                cancellationToken).ConfigureAwait(false);

            FilePortResponse response = await ReadResponseAsync(networkStream, cancellationToken).ConfigureAwait(false);
            EnsureAccepted(response, "File download was rejected by the server.");

            long expectedBytes = response.FileSize;
            ValidateFileSize(expectedBytes);

            await using (var output = new FileStream(
                partPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                BufferSize,
                useAsync: true))
            {
                var buffer = new byte[BufferSize];
                long transferred = 0;
                ReportProgress(progress, transferred, expectedBytes);

                while (transferred < expectedBytes)
                {
                    int bytesToRead = (int)Math.Min(buffer.Length, expectedBytes - transferred);
                    int read = await networkStream.ReadAsync(buffer.AsMemory(0, bytesToRead), cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                    {
                        throw new EndOfStreamException("Server closed the connection before the file was fully downloaded.");
                    }

                    await output.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                    transferred += read;
                    ReportProgress(progress, transferred, expectedBytes);
                }

                await output.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            File.Move(partPath, request.DestinationPath, overwrite: true);
        }
        catch
        {
            SafeDelete(partPath);
            throw;
        }
    }

    private static async Task<FilePortResponse> ReadResponseAsync(NetworkStream networkStream, CancellationToken cancellationToken)
    {
        FilePortResponse? response = await FileTransferProtocol.ReadJsonLineAsync<FilePortResponse>(networkStream, cancellationToken)
            .ConfigureAwait(false);

        if (response == null)
        {
            throw new IOException("Server closed the connection before sending a response.");
        }

        return response;
    }

    private static void EnsureAccepted(FilePortResponse response, string defaultMessage)
    {
        if (response.IsSuccess)
        {
            return;
        }

        string message = string.IsNullOrWhiteSpace(response.ErrorText) ? defaultMessage : response.ErrorText;
        throw new InvalidOperationException(message);
    }

    private static void ValidateEndpoint(string serverIp, int filePort)
    {
        ValidateRequired(serverIp, nameof(serverIp));
        if (filePort <= 0 || filePort > 65_535)
        {
            throw new ArgumentOutOfRangeException(nameof(filePort), "File port must be between 1 and 65535.");
        }
    }

    private static void ValidateRequired(string value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Value is required.", name);
        }
    }

    private static void ValidateFileSize(long fileSize)
    {
        if (fileSize < 1)
        {
            throw new InvalidOperationException("File must contain at least one byte.");
        }

        if (fileSize > MaxFileSizeBytes)
        {
            throw new InvalidOperationException("File exceeds the 10 GB size limit.");
        }
    }

    private static void ReportProgress(
        IProgress<FileTransferProgress>? progress,
        long bytesTransferred,
        long totalBytes)
    {
        double percent = totalBytes <= 0 ? 0 : Math.Min(100, bytesTransferred * 100d / totalBytes);
        progress?.Report(new FileTransferProgress(bytesTransferred, totalBytes, percent));
    }

    private static void SafeDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
        }
    }
}
