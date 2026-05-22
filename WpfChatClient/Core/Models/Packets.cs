using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace WpfChatClient.Core.Models;

public enum PacketType
{
    Join,
    Leave,
    ChatMessage,
    Typing,
    UserListUpdate,
    SystemMessage,
    Heartbeat,
    PrivateMessage,
    RoomJoin,
    ConnectionRejected,
    FileOffer,
    FileAvailable,
    FileTransferFailed
}

public class Packet
{
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public PacketType Type { get; set; }
    public JsonElement Data { get; set; }
}

public class JoinData { public string Username { get; set; } = string.Empty; }

public class ChatMessageData
{
    public string MessageId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public string Timestamp { get; set; } = string.Empty;
}

public class PrivateMessageData
{
    public string MessageId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string Recipient { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public string Timestamp { get; set; } = string.Empty;
}

public class RoomJoinData
{
    public string RoomId { get; set; } = string.Empty;
}

public class TypingData
{
    public string Username { get; set; } = string.Empty;
    public bool IsTyping { get; set; }
}

public class UserListUpdateData { public List<string> Users { get; set; } = new(); }
public class SystemMessageData { public string Message { get; set; } = string.Empty; }
public class HeartbeatData { }

public class FileOfferData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public string PreviewBase64 { get; set; } = string.Empty;
    public string PreviewMime { get; set; } = string.Empty;
    public string CreatedAt { get; set; } = string.Empty;
}

public class FileAvailableData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
}

public class FileTransferFailedData
{
    public string TransferId { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
}
