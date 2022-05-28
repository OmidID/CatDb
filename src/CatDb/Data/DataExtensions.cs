using System.Linq.Expressions;

namespace CatDb.Data
{
    public static class DataExtensions
    {
        public static Expression Value(this Expression data)
        {
            return Expression.Field(data, "Value");
        }
    }
}
