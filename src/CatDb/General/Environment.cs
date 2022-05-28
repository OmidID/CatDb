namespace CatDb.General
{
    public static class Environment
    {
        public static readonly bool RunningOnMono = Type.GetType("Mono.Runtime") != null;
    }
}
