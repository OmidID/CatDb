namespace CatDb.Database
{
    public static class StructureType
    {
        //do not change
        public const int RESERVED = 0;

        public const int XTABLE = 1;
        public const int XFILE = 2;

        public static bool IsValid(int type)
        {
            return type is XTABLE or XFILE;
        }
    }
}
