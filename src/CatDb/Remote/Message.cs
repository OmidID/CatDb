#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using CatDb.Data;
using CatDb.Remote.Commands;
using CatDb.WaterfallTree;

namespace CatDb.Remote;
///<summary>
///--------------------- Message Exchange Protocol
///
///--------------------- Comments-----------------------------------
///Format           : binary
///Byte style       : LittleEndian
///String Encoding  : Unicode (UTF-8) 
///String format    : string int size compressed with 7-bit encoding, byte[] Unicode (UTF-8)
///
///------------------------------------------------------------------
///ID                : Long ID
///     
///Commands          : CommandCollection
///
///</summary>    
public class Message
{
    public string? DatabaseName { get; }
    public string? UserName { get; }
    public string? Password { get; }
    public IDescriptor Description { get; private set; }
    public CommandCollection Commands { get; private set; }

    public Message(IDescriptor description, CommandCollection commands, string? databaseName = null, string? userName = null, string? password = null)
    {
        DatabaseName = databaseName;
        UserName = userName;
        Password = password;
        Description = description;
        Commands = commands;
    }

    public void Serialize(BinaryWriter writer)
    {
        WriteNullableString(writer, DatabaseName);
        WriteNullableString(writer, UserName);
        WriteNullableString(writer, Password);

        var id = Description.Id;

        writer.Write(id);

        var persist = id > 0 ? new CommandPersist(new DataPersist(Description.KeyType, null, AllowNull.OnlyMembers), new DataPersist(Description.RecordType, null, AllowNull.OnlyMembers)) : new CommandPersist(null, null);
        var commandsPersist = new CommandCollectionPersist(persist);

        commandsPersist.Write(writer, Commands);
    }

    public static Message Deserialize(BinaryReader reader, Func<string?, string?, string?, long, IDescriptor> find)
    {
        var databaseName = ReadNullableString(reader);
        var userName = ReadNullableString(reader);
        var password = ReadNullableString(reader);

        var id = reader.ReadInt64();

        IDescriptor description = null;
        var persist = new CommandPersist(null, null);

        if (id > 0)
        {
            try
            {
                description = find(databaseName, userName, password, id);
                persist = new CommandPersist(new DataPersist(description.KeyType, null, AllowNull.OnlyMembers), new DataPersist(description.RecordType, null, AllowNull.OnlyMembers));
            }
            catch (Exception)
            {
                throw new Exception("Cannot find description with the specified ID");
            }
        }
        
        var commandsPersist = new CommandCollectionPersist(persist);
        var commands = commandsPersist.Read(reader);

        return new Message(description, commands, databaseName, userName, password);
    }

    private static void WriteNullableString(BinaryWriter writer, string? value)
    {
        writer.Write(value != null);
        if (value != null)
            writer.Write(value);
    }

    private static string? ReadNullableString(BinaryReader reader)
    {
        return reader.ReadBoolean() ? reader.ReadString() : null;
    }
}
