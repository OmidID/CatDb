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
    public IDescriptor Description { get; private set; }
    public CommandCollection Commands { get; private set; }

    public Message(IDescriptor description, CommandCollection commands)
    {
        Description = description;
        Commands = commands;
    }

    public void Serialize(BinaryWriter writer)
    {
        var id = Description.Id;

        writer.Write(id);

        var persist = id > 0 ? new CommandPersist(new DataPersist(Description.KeyType, null, AllowNull.OnlyMembers), new DataPersist(Description.RecordType, null, AllowNull.OnlyMembers)) : new CommandPersist(null, null);
        var commandsPersist = new CommandCollectionPersist(persist);

        commandsPersist.Write(writer, Commands);
    }

    public static Message Deserialize(BinaryReader reader, Func<long, IDescriptor> find)
    {
        var id = reader.ReadInt64();

        IDescriptor description = null;
        var persist = new CommandPersist(null, null);

        if (id > 0)
        {
            try
            {
                description = find(id);
                persist = new CommandPersist(new DataPersist(description.KeyType, null, AllowNull.OnlyMembers), new DataPersist(description.RecordType, null, AllowNull.OnlyMembers));
            }
            catch (Exception)
            {
                throw new Exception("Cannot find description with the specified ID");
            }
        }
        
        var commandsPersist = new CommandCollectionPersist(persist);
        var commands = commandsPersist.Read(reader);

        return new Message(description, commands);
    }
}
