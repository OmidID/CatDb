namespace CatDb.Data
{
    public interface IToObjects<T> : ITransformer<T, object[]>
    {
    }
}
