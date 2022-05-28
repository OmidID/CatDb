using CatDb.General.Comparers;

namespace CatDb.General.IO
{
    public class AtomicFile
    {
        private byte[] HEADER = new byte[512];
        private CommonArray commonArray = new CommonArray();

        private Stream stream;
        public string FileName { get; private set; }

        public AtomicFile(string fileName)
        {
            commonArray.ByteArray = HEADER;

            stream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            FileName = fileName;

            if (stream.Length < HEADER.Length)
            {
                Pos = HEADER.Length;
                Size = 0;
                stream.Write(HEADER, 0, HEADER.Length);
            }
            else
                stream.Read(HEADER, 0, HEADER.Length);
        }

        private long Pos
        {
            get => commonArray.Int64Array[0];
            set => commonArray.Int64Array[0] = value;
        }

        public int Size
        {
            get => (int)commonArray.Int64Array[1];
            private set => commonArray.Int64Array[1] = value;
        }

        public void Write(byte[] buffer, int index, int count)
        {
            if (Pos - 1 - HEADER.Length >= count)
                Pos = HEADER.Length;
            else
                Pos = Pos + Size;

            Size = count;

            stream.Seek(Pos, SeekOrigin.Begin);
            stream.Write(buffer, index, count);

            stream.Seek(0, SeekOrigin.Begin);
            stream.Write(HEADER, 0, 2 * sizeof(long)); //HEADER.Length
            stream.Flush();
        }

        public void Write(byte[] buffer)
        {
            Write(buffer, 0, buffer.Length);
        }

        public byte[] Read()
        {
            stream.Seek(Pos, SeekOrigin.Begin);

            var buffer = new byte[Size];
            var readed = stream.Read(buffer, 0, buffer.Length);

            if (readed != buffer.Length)
                throw new IOException(); //should never happen

            return buffer;
        }

        public void Close()
        {
            if (Pos + Size < stream.Length)
                stream.SetLength(Pos + Size);

            stream.Close();
        }

        public long Length => stream.Length;
    }
}
