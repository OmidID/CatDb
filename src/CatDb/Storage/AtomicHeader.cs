namespace CatDb.Storage
{
    public class AtomicHeader
    {
        private const string TITLE = "CatDb 4.0";
        /// <summary>
        /// http://en.wikipedia.org/wiki/Advanced_Format
        /// http://www.idema.org
        /// </summary>
        public const int SIZE = 4 * 1024;
        public const int MAX_TAG_DATA = 256;

        private byte[] _tag;
        private int _version;
        public bool UseCompression;

        /// <summary>
        /// System data location.
        /// </summary>
        public Ptr SystemData;

        public void Serialize(Stream stream)
        {
            var buffer = new byte[SIZE];

            using (var ms = new MemoryStream(buffer))
            {
                var writer = new BinaryWriter(ms);
                writer.Write(TITLE);

                writer.Write(_version);
                writer.Write(UseCompression);

                //last flush location
                SystemData.Serialize(writer);

                //tag
                if (Tag == null)
                    writer.Write(-1);
                else
                {
                    writer.Write(Tag.Length);
                    writer.Write(Tag);
                }
            }

            stream.Seek(0, SeekOrigin.Begin);
            stream.Write(buffer, 0, buffer.Length);
        }

        public static AtomicHeader Deserialize(Stream stream)
        {
            var header = new AtomicHeader();

            stream.Seek(0, SeekOrigin.Begin);

            var buffer = new byte[SIZE];
            if (stream.Read(buffer, 0, SIZE) != SIZE)
                throw new Exception($"Invalid {TITLE} header.");

            using var ms = new MemoryStream(buffer);
            var reader = new BinaryReader(ms);

            var title = reader.ReadString();
            if (title != TITLE)
                throw new Exception($"Invalid {TITLE} header.");

            header._version = reader.ReadInt32();
            header.UseCompression = reader.ReadBoolean();

            //last flush location
            header.SystemData = Ptr.Deserialize(reader);

            //tag
            var tagLength = reader.ReadInt32();
            header.Tag = tagLength >= 0 ? reader.ReadBytes(tagLength) : null;

            return header;
        }

        public byte[] Tag
        {
            get => _tag;
            set
            {
                if (value is { Length: > MAX_TAG_DATA })
                    throw new ArgumentException("Tag");

                _tag = value;
            }
        }
    }
}
