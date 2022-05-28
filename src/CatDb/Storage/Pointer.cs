namespace CatDb.Storage
{
    public class Pointer
    {
        public long Version;
        public Ptr Ptr;

        public bool IsReserved;
        public int RefCount;

        public Pointer(long version, Ptr ptr)
        {
            Version = version;
            Ptr = ptr;
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Version);
            Ptr.Serialize(writer);
        }

        public static Pointer Deserialize(BinaryReader reader)
        {
            var version = reader.ReadInt64();
            var ptr = Ptr.Deserialize(reader);

            return new Pointer(version, ptr);
        }

        public override string ToString()
        {
            return $"Version {Version}, Ptr {Ptr}";
        }
    }
}
