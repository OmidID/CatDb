using System.Linq.Expressions;

namespace CatDb.General.Extensions
{
    public class ListHelper<T>
    {
        public static readonly ListHelper<T> Instance = new();

        public Action<List<T>, T[]> SetArray { get; private set; }
        public Func<List<T>, T[]> GetArray { get; private set; }
        public Action<List<T>, int> SetCount { get; private set; }
        public Action<List<T>> IncrementVersion { get; private set; }

        public ListHelper()
        {
            var setArrayLambda = CreateSetArrayMethod();
            SetArray = setArrayLambda.Compile();

            var getArrayLambda = CreateGetArrayMethod();
            GetArray = getArrayLambda.Compile();

            var setCountLambda = CreateSetCountMethod();
            SetCount = setCountLambda.Compile();

            var incrementVersionLambda = CreateIncremetVersionMethod();
            IncrementVersion = incrementVersionLambda.Compile();
        }

        public Expression<Action<List<T>, T[]>> CreateSetArrayMethod()
        {
            var list = Expression.Parameter(typeof(List<T>), "list");
            var array = Expression.Parameter(typeof(T[]), "array");

            var assign = Expression.Assign(Expression.PropertyOrField(list, "_items"), array);

            return Expression.Lambda<Action<List<T>, T[]>>(assign, list, array);
        }

        public Expression<Func<List<T>, T[]>> CreateGetArrayMethod()
        {
            var list = Expression.Parameter(typeof(List<T>), "list");
            var items = Expression.PropertyOrField(list, "_items");

            return Expression.Lambda<Func<List<T>, T[]>>(Expression.Label(Expression.Label(typeof(T[])), items), list);
        }

        public Expression<Action<List<T>, int>> CreateSetCountMethod()
        {
            var list = Expression.Parameter(typeof(List<T>), "list");
            var count = Expression.Parameter(typeof(int), "count");

            var assign = Expression.Assign(Expression.PropertyOrField(list, "_size"), count);

            return Expression.Lambda<Action<List<T>, int>>(assign, list, count);
        }

        public Expression<Action<List<T>>> CreateIncremetVersionMethod()
        {
            var list = Expression.Parameter(typeof(List<T>), "list");

            var version = Expression.PropertyOrField(list, "_version");
            var assign = Expression.Assign(version, Expression.Add(version, Expression.Constant(1, typeof(int))));

            return Expression.Lambda<Action<List<T>>>(assign, list);
        }
    }
}
