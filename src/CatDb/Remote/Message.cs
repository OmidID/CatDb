using CatDb.Data;
using CatDb.WaterfallTree;
using CatDb.Remote.Commands;

namespace CatDb.Remote
{
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

        private static KeyValuePair<long, IDescriptor> _previousRecord = new(-1, null);

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
                    description = _previousRecord.Key == id ? _previousRecord.Value : find(id);
                    persist = new CommandPersist(new DataPersist(description.KeyType, null, AllowNull.OnlyMembers), new DataPersist(description.RecordType, null, AllowNull.OnlyMembers));
                }
                catch (Exception exc)
                {
                    throw new Exception("Cannot find description with the specified ID");
                }

                if (_previousRecord.Key != id)
                    _previousRecord = new KeyValuePair<long, IDescriptor>(id, description);
            }
            
            var commandsPersist = new CommandCollectionPersist(persist);
            var commands = commandsPersist.Read(reader);

            return new Message(description, commands);
        }
    }
}