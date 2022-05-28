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

        private static KeyValuePair<long, IDescriptor> PreviousRecord = new KeyValuePair<long, IDescriptor>(-1, null);

        public Message(IDescriptor description, CommandCollection commands)
        {
            Description = description;
            Commands = commands;
        }

        public void Serialize(BinaryWriter writer)
        {
            var ID = Description.ID;

            writer.Write(ID);

            var persist = ID > 0 ? new CommandPersist(new DataPersist(Description.KeyType, null, AllowNull.OnlyMembers), new DataPersist(Description.RecordType, null, AllowNull.OnlyMembers)) : new CommandPersist(null, null);
            var commandsPersist = new CommandCollectionPersist(persist);

            commandsPersist.Write(writer, Commands);
        }

        public static Message Deserialize(BinaryReader reader, Func<long, IDescriptor> find)
        {
            var ID = reader.ReadInt64();

            IDescriptor description = null;
            var persist = new CommandPersist(null, null);

            if (ID > 0)
            {
                try
                {
                    description = PreviousRecord.Key == ID ? PreviousRecord.Value : find(ID);
                    persist = new CommandPersist(new DataPersist(description.KeyType, null, AllowNull.OnlyMembers), new DataPersist(description.RecordType, null, AllowNull.OnlyMembers));
                }
                catch (Exception exc)
                {
                    throw new Exception("Cannot find description with the specified ID");
                }

                if (PreviousRecord.Key != ID)
                    PreviousRecord = new KeyValuePair<long, IDescriptor>(ID, description);
            }
            
            var commandsPersist = new CommandCollectionPersist(persist);
            var commands = commandsPersist.Read(reader);

            return new Message(description, commands);
        }
    }
}