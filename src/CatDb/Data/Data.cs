namespace CatDb.Data;
public class Data<T> : IData
{
    public T Value = default!;

    public Data()
    {
    }

    public Data(T value)
    {
        Value = value;
    }

    public override string ToString()
    {
        return Value?.ToString() ?? string.Empty;
    }
}
