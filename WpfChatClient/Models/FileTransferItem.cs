using System.Globalization;
using CommunityToolkit.Mvvm.ComponentModel;

namespace WpfChatClient.Models;

public partial class FileTransferItem : ObservableObject
{
    public const long MaxFileSizeBytes = 10L * 1024 * 1024 * 1024;

    [ObservableProperty]
    private string _transferId = string.Empty;

    [ObservableProperty]
    private string _fileName = string.Empty;

    [ObservableProperty]
    private long _fileSize;

    [ObservableProperty]
    private string _previewBase64 = string.Empty;

    [ObservableProperty]
    private string _previewMime = string.Empty;

    [ObservableProperty]
    private string _sender = string.Empty;

    [ObservableProperty]
    private string _roomId = string.Empty;

    [ObservableProperty]
    private FileTransferUiStatus _status = FileTransferUiStatus.Pending;

    [ObservableProperty]
    private double _progress;

    [ObservableProperty]
    private string _localPath = string.Empty;

    [ObservableProperty]
    private string _statusText = "Waiting";

    [ObservableProperty]
    private string _errorMessage = string.Empty;

    [ObservableProperty]
    private bool _isOwn;

    public string SizeText => FormatFileSize(FileSize);
    public bool IsBusy => Status is FileTransferUiStatus.Uploading or FileTransferUiStatus.Downloading;
    public bool CanDownload => Status == FileTransferUiStatus.Available && !IsOwn;
    public bool CanCancel => Status is FileTransferUiStatus.Uploading or FileTransferUiStatus.Downloading;
    public bool HasPreview => !string.IsNullOrWhiteSpace(PreviewBase64);

    public static string FormatFileSize(long bytes)
    {
        if (bytes < 1024)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{bytes} B");
        }

        double value = bytes / 1024d;
        if (value < 1024)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{value:0.#} KB");
        }

        value /= 1024d;
        if (value < 1024)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{value:0.#} MB");
        }

        value /= 1024d;
        return string.Create(CultureInfo.InvariantCulture, $"{value:0.#} GB");
    }

    partial void OnFileSizeChanged(long value)
    {
        OnPropertyChanged(nameof(SizeText));
    }

    partial void OnStatusChanged(FileTransferUiStatus value)
    {
        OnPropertyChanged(nameof(IsBusy));
        OnPropertyChanged(nameof(CanDownload));
        OnPropertyChanged(nameof(CanCancel));
    }

    partial void OnPreviewBase64Changed(string value)
    {
        OnPropertyChanged(nameof(HasPreview));
    }

    partial void OnIsOwnChanged(bool value)
    {
        OnPropertyChanged(nameof(CanDownload));
    }
}
