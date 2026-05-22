using System.Collections.Concurrent;

namespace ChatServer.FileTransfers;

public sealed class FileMetadataStore
{
    public const long MaxFileSizeBytes = 10L * 1024 * 1024 * 1024;
    public const int BufferSizeBytes = 4_194_304;

    private static readonly TimeSpan DefaultPartialMaxAge = TimeSpan.FromHours(24);

    private readonly ConcurrentDictionary<string, FileTransferRecord> _records = new(StringComparer.Ordinal);
    private readonly string _partialDirectory;

    public FileMetadataStore(string storageRoot)
    {
        if (string.IsNullOrWhiteSpace(storageRoot))
        {
            throw new ArgumentException("Storage root cannot be blank.", nameof(storageRoot));
        }

        StorageRoot = Path.GetFullPath(storageRoot);
        _partialDirectory = Path.Combine(StorageRoot, ".partial");

        Directory.CreateDirectory(StorageRoot);
        Directory.CreateDirectory(_partialDirectory);
    }

    public string StorageRoot { get; }

    public bool TryRegisterOffer(
        FileOfferData offer,
        string authenticatedSender,
        string senderAddress,
        out FileTransferRecord record,
        out string failureReason)
    {
        record = null!;
        failureReason = string.Empty;

        if (string.IsNullOrWhiteSpace(offer.TransferId))
        {
            failureReason = "Transfer ID is required.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(offer.FileName))
        {
            failureReason = "File name is required.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(offer.RoomId))
        {
            failureReason = "Room ID is required.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(authenticatedSender) ||
            !string.Equals(offer.Sender, authenticatedSender, StringComparison.OrdinalIgnoreCase))
        {
            failureReason = "Sender does not match the authenticated session.";
            return false;
        }

        if (offer.FileSize < 1 || offer.FileSize > MaxFileSizeBytes)
        {
            failureReason = $"File size must be between 1 byte and {MaxFileSizeBytes} bytes.";
            return false;
        }

        if (!TryNormalizeTransferId(offer.TransferId, out var transferId))
        {
            failureReason = "Transfer ID must be a GUID.";
            return false;
        }

        var roomId = offer.RoomId.Trim();
        var safeFileName = SanitizeFileName(offer.FileName);
        if (string.IsNullOrWhiteSpace(safeFileName))
        {
            failureReason = "File name is invalid.";
            return false;
        }

        var candidate = new FileTransferRecord
        {
            TransferId = transferId,
            SafeTransferId = transferId,
            RoomId = roomId,
            Sender = authenticatedSender,
            SenderAddress = senderAddress,
            OriginalFileName = offer.FileName,
            SafeFileName = safeFileName,
            FileSize = offer.FileSize
        };

        if (!_records.TryAdd(candidate.TransferId, candidate))
        {
            failureReason = "Transfer ID is already registered.";
            return false;
        }

        record = candidate;
        return true;
    }

    public bool TryGet(string transferId, out FileTransferRecord record)
    {
        if (TryNormalizeTransferId(transferId, out var normalizedTransferId) &&
            _records.TryGetValue(normalizedTransferId, out var found))
        {
            record = found;
            return true;
        }

        record = null!;
        return false;
    }

    public bool TryClaimUpload(string transferId, out FileTransferRecord record, out string failureReason)
    {
        failureReason = string.Empty;
        if (!TryGet(transferId, out record) || record == null)
        {
            failureReason = "Transfer is not registered.";
            return false;
        }

        lock (record)
        {
            if (record.Status != FileTransferStatus.Pending)
            {
                failureReason = $"Transfer is {record.Status}.";
                return false;
            }

            record.Status = FileTransferStatus.Uploading;
            return true;
        }
    }

    public bool TryMarkAvailable(string transferId, string completedFilePath, out FileTransferRecord record)
    {
        if (!TryGet(transferId, out record) || record == null)
        {
            return false;
        }

        lock (record)
        {
            record.Status = FileTransferStatus.Available;
            record.FinalPath = completedFilePath;
            record.AvailableUtc = DateTime.UtcNow;
            record.FailureReason = null;
            record.FailedUtc = null;
        }

        return true;
    }

    public bool TryMarkFailed(string transferId, string failureReason, out FileTransferRecord record)
    {
        if (!TryGet(transferId, out record) || record == null)
        {
            return false;
        }

        lock (record)
        {
            record.Status = FileTransferStatus.Failed;
            record.FailureReason = failureReason;
            record.FailedUtc = DateTime.UtcNow;
        }

        return true;
    }

    public string GetPartialUploadPath(string transferId)
    {
        if (!TryGet(transferId, out var record) || record == null)
        {
            throw new InvalidOperationException("Transfer is not registered.");
        }

        return GetPartialUploadPath(record);
    }

    public string GetPartialUploadPath(FileTransferRecord record)
    {
        Directory.CreateDirectory(_partialDirectory);
        return Path.Combine(_partialDirectory, $"{record.SafeTransferId}.part");
    }

    public string GetCompletedDirectory(string transferId)
    {
        if (!TryGet(transferId, out var record) || record == null)
        {
            throw new InvalidOperationException("Transfer is not registered.");
        }

        return GetCompletedDirectory(record);
    }

    public string GetCompletedDirectory(FileTransferRecord record)
    {
        return Path.Combine(StorageRoot, record.SafeTransferId);
    }

    public void CleanupStalePartialFiles()
    {
        CleanupStalePartialFiles(DefaultPartialMaxAge);
    }

    public int CleanupStalePartialFiles(TimeSpan maxAge)
    {
        Directory.CreateDirectory(_partialDirectory);

        var cutoffUtc = DateTime.UtcNow - maxAge;
        var deleted = 0;
        foreach (var filePath in Directory.EnumerateFiles(_partialDirectory, "*.part", SearchOption.TopDirectoryOnly))
        {
            try
            {
                var lastWriteUtc = File.GetLastWriteTimeUtc(filePath);
                if (lastWriteUtc >= cutoffUtc)
                {
                    continue;
                }

                File.Delete(filePath);
                deleted++;
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        return deleted;
    }

    public static string SanitizeFileName(string fileName)
    {
        return SanitizePathSegment(Path.GetFileName(fileName.Trim()));
    }

    private static bool TryNormalizeTransferId(string transferId, out string normalizedTransferId)
    {
        normalizedTransferId = string.Empty;
        if (string.IsNullOrWhiteSpace(transferId))
        {
            return false;
        }

        if (!Guid.TryParse(transferId.Trim(), out var guid))
        {
            return false;
        }

        normalizedTransferId = guid.ToString("N");
        return true;
    }

    private static string SanitizePathSegment(string segment)
    {
        if (segment == "." || segment == "..")
        {
            return string.Empty;
        }

        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitized = new char[segment.Length];

        for (var i = 0; i < segment.Length; i++)
        {
            var current = segment[i];
            sanitized[i] = invalidChars.Contains(current) ? '_' : current;
        }

        var result = new string(sanitized).Trim();
        return result is "." or ".." ? string.Empty : result;
    }
}
