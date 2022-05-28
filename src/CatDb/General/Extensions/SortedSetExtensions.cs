using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.General.Extensions
{
    public class SortedSetHelper<T>
    {
        public static readonly SortedSetHelper<T> Instance = new();

        private KeyValuePair<bool, T> Find(SortedSet<T> set, T key)
        {
            var node = set.FindNode(key); //private method invocation..
            if (node != null)
                return new KeyValuePair<bool, T>(true, node.Item);

            return new KeyValuePair<bool, T>(false, default(T));
        }

        public void ConstructFromSortedArray(SortedSet<T> set, T[] array, int index, int count)
        {
            set.Root = SortedSet<T>.ConstructRootFromSortedArray(array, index, index + count - 1, null); //private method invocation..
            set.Count = count;
            set.Version++;
        }

        public bool Replace(SortedSet<T> set, T item, Func<T, T, T> onExist)
        {
            if (set.Root == null)
            {
                set.Root = new SortedSet<T>.Node(item, NodeColor.Black);
                set.Count = 1;
                set.Version++;
                return false;
            }

            var root = set.Root;
            SortedSet<T>.Node node = null;
            SortedSet<T>.Node grandParent = null;
            SortedSet<T>.Node greatGrandParent = null;
            set.Version++;
            var cmp = 0;
            var comparer = set.Comparer;

            while (root != null)
            {
                cmp = comparer.Compare(item, root.Item);
                if (cmp == 0)
                {
                    set.Root.Color = NodeColor.Black;
                    root.Item = onExist != null ? onExist(root.Item, item) : item;
                    return true;
                }

                if (root.Is4Node)
                {
                    root.Split4Node();
                    if (node.IsRed)
                        set.InsertionBalance(root, ref node, grandParent, greatGrandParent);
                }

                greatGrandParent = grandParent;
                grandParent = node;
                node = root;
                root = (cmp < 0) ? root.Left : root.Right;
            }

            var current = new SortedSet<T>.Node(item, NodeColor.Black);
            if (cmp > 0)
                node.Right = current;
            else
                node.Left = current;

            if (node.IsRed)
                set.InsertionBalance(current, ref node, grandParent, greatGrandParent);

            set.Root.Color = NodeColor.Black;
            set.Count++;
            return false;
        }
        public SortedSet<T> GetViewBetween(SortedSet<T> set, T lowerValue, T upperValue, bool lowerBoundActive, bool upperBoundActive)
        {
            if ((lowerBoundActive && upperBoundActive) && set.Comparer.Compare(lowerValue, upperValue) > 0)
                throw new ArgumentException("lowerBound is greater than upperBound");

            return new SortedSet<T>.TreeSubSet(set, lowerValue, upperValue, lowerBoundActive, lowerBoundActive);
        }
        
        public KeyValuePair<bool, T> FindNext(SortedSet<T> set, T item)
        {
            int cmp;
            SortedSet<T>.Node next = null;
            var node = set.Root;

            while (node != null)
            {
                cmp = set.Comparer.Compare(item, node.Item);

                if (cmp == 0)
                    return new KeyValuePair<bool, T>(true, node.Item);

                if (cmp < 0)
                {
                    next = node;
                    node = node.Left;
                }
                else
                    node = node.Right;
            }

            if (next != null)
                return new KeyValuePair<bool, T>(true, next.Item);

            return new KeyValuePair<bool, T>(false, default(T));
        }

        public KeyValuePair<bool, T> FindPrev(SortedSet<T> set, T item)
        {
            int cmp;
            SortedSet<T>.Node prev = null;
            var node = set.Root;

            while (node != null)
            {
                cmp = set.Comparer.Compare(item, node.Item);

                if (cmp == 0)
                    return new KeyValuePair<bool, T>(true, node.Item);

                if (cmp > 0)
                {
                    prev = node;
                    node = node.Right;
                }
                else
                    node = node.Left;
            }

            if (prev != null)
                return new KeyValuePair<bool, T>(true, prev.Item);

            return new KeyValuePair<bool, T>(false, default(T));
        }

        public KeyValuePair<bool, T> FindAfter(SortedSet<T> set, T item)
        {
            int cmp;
            SortedSet<T>.Node next = null;
            var node = set.Root;

            while (node != null)
            {
                cmp = set.Comparer.Compare(item, node.Item);

                if (cmp == 0)
                {
                    if (node.Right != null)
                    {
                        var tmp = node.Right;

                        while (tmp != null)
                        {
                            next = tmp;
                            tmp = tmp.Left;
                        }
                    }

                    break;
                }

                if (cmp < 0)
                {
                    next = node;
                    node = node.Left;
                }
                else
                    node = node.Right;
            }

            if (next != null)
                return new KeyValuePair<bool, T>(true, next.Item);

            return new KeyValuePair<bool, T>(false, default(T));
        }

        public KeyValuePair<bool, T> FindBefore(SortedSet<T> set, T item)
        {
            int cmp;
            SortedSet<T>.Node prev = null;
            var node = set.Root;

            while (node != null)
            {
                cmp = set.Comparer.Compare(item, node.Item);

                if (cmp == 0)
                {
                    if (node.Left != null)
                    {
                        var tmp = node.Left;

                        while (tmp != null)
                        {
                            prev = tmp;
                            tmp = tmp.Right;
                        }
                    }

                    break;
                }

                if (cmp > 0)
                {
                    prev = node;
                    node = node.Right;
                }
                else
                    node = node.Left;
            }

            if (prev != null)
                return new KeyValuePair<bool, T>(true, prev.Item);

            return new KeyValuePair<bool, T>(false, default(T));
        }

        public bool TryGetValue(SortedSet<T> set, T key, out T value)
        {
            var kv = Find(set, key);

            if (!kv.Key)
            {
                value = default(T);
                return false;
            }

            value = kv.Value;
            return true;
        }

    }

    public class SortedSetHelperAa<T>
    {
        public static readonly SortedSetHelper<T> Instance = new();

        private readonly Func<SortedSet<T>, T, KeyValuePair<bool, T>> _find;
        private readonly Action<SortedSet<T>, T[], int, int> _constructFromSortedArray;
        private readonly Func<SortedSet<T>, T, Func<T, T, T>, bool> _replace;
        private readonly Func<SortedSet<T>, T, T, bool, bool, SortedSet<T>> _getViewBetween;

        private readonly Func<SortedSet<T>, T, KeyValuePair<bool, T>> _findNext;
        private readonly Func<SortedSet<T>, T, KeyValuePair<bool, T>> _findPrev;
        private readonly Func<SortedSet<T>, T, KeyValuePair<bool, T>> _findAfter;
        private readonly Func<SortedSet<T>, T, KeyValuePair<bool, T>> _findBefore;

        public SortedSetHelperAa()
        {
            var lambdaFind = CreateFindMethod();
            var lambdaConstructFromSortedArray = CreateConstructFromSortedArrayMethod();
            var lambdaReplace = CreateReplaceMethod();
            var lambdaGetViewBetween = CreateGetViewBetweenMethod();

            _find = lambdaFind.Compile();
            _constructFromSortedArray = lambdaConstructFromSortedArray.Compile();
            _replace = lambdaReplace.Compile();
            _getViewBetween = lambdaGetViewBetween.Compile();
            _findNext = CreateFindNextMethod().Compile();
            _findPrev = CreateFindPrevMethod().Compile();
            _findAfter = CreateFindAfterMethod().Compile();
            _findBefore = CreateFindBeforeMethod().Compile();
        }

        public Expression<Func<SortedSet<T>, T, KeyValuePair<bool, T>>> CreateFindMethod()
        {
            var type = typeof(SortedSet<T>);
            var nodeType = CreateNode(typeof(T));

            var set = Expression.Variable(typeof(SortedSet<T>), "set");
            var key = Expression.Variable(typeof(T), "key");

            var exitPoint = Expression.Label(typeof(KeyValuePair<bool, T>));

            MethodInfo findNode;
#if NETFX_CORE
            findNode = type.GetMethod("FindNode");
#else
            findNode = type.GetMethod("FindNode", BindingFlags.NonPublic | BindingFlags.Instance);
#endif
            var call = Expression.Call(set, findNode, key);
            var node = Expression.Variable(nodeType, "node");
            var assign = Expression.Assign(node, call);

            var contructInfo = typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) });
            var newkvTrue = Expression.New(contructInfo, Expression.Constant(true, typeof(bool)), Expression.PropertyOrField(node, "Item"));
            var newkvFalse = Expression.New(contructInfo, Expression.Constant(false, typeof(bool)), Expression.Default(typeof(T)));

            var @if = Expression.IfThen(Expression.NotEqual(node, Expression.Constant(null)),
                Expression.Return(exitPoint, newkvTrue));

            var @return = Expression.Label(exitPoint, newkvFalse);

            var body = Expression.Block(typeof(KeyValuePair<bool, T>), new[] { node }, assign, @if, @return);

            //private KeyValuePair<bool, T> Find(SortedSet<T> set, T key)
            //{
            //    var node = set.FindNode(key); //private method invocation..
            //    if (node != null)
            //        return new KeyValuePair<bool, T>(true, node.Item);

            //    return new KeyValuePair<bool, T>(false, default(T));
            //}

            return Expression.Lambda<Func<SortedSet<T>, T, KeyValuePair<bool, T>>>(body, set, key);
        }

        public Expression<Action<SortedSet<T>, T[], int, int>> CreateConstructFromSortedArrayMethod()
        {
            var type = typeof(SortedSet<T>);
            var nodeType = CreateNode(typeof(T));

            var set = Expression.Variable(typeof(SortedSet<T>), "set");
            var array = Expression.Variable(typeof(T[]), "array");
            var index = Expression.Variable(typeof(int), "index");
            var count = Expression.Variable(typeof(int), "count");

            MethodInfo method;
#if NETFX_CORE
            method = type.GetMethod("ConstructRootFromSortedArray");
#else
            method = type.GetMethod("ConstructRootFromSortedArray", BindingFlags.NonPublic | BindingFlags.Static);
#endif
            var toIndex = Expression.Subtract(Expression.Add(index, count), Expression.Constant(1, typeof(int)));
            var call = Expression.Call(method, array, index, toIndex, Expression.Constant(null, nodeType));

            var setRoot = Expression.Assign(Expression.Field(set, "root"), call);
            var setCount = Expression.Assign(Expression.Field(set, "count"), count);
            var incVersion = Expression.Increment(Expression.Field(set, "version"));

            var body = Expression.Block(setRoot, setCount, incVersion);

            //private void ConstructFromSortedArray(SortedSet<T> set, T[] array, int index, int count)
            //{
            //    set.root = SortedSet<T>.ConstructRootFromSortedArray(array, index, index + count - 1, null); //private method invocation..
            //    set.count = count;
            //    set.version++;
            //}

            return Expression.Lambda<Action<SortedSet<T>, T[], int, int>>(body, set, array, index, count);
        }

        public Expression<Func<SortedSet<T>, T, Func<T, T, T>, bool>> CreateReplaceMethod()
        {
            var set = Expression.Parameter(typeof(SortedSet<T>), "set");
            var item = Expression.Parameter(typeof(T), "item");
            var onExist = Expression.Parameter(typeof(Func<T, T, T>), "onExist");

            var exitPoint = Expression.Label(typeof(bool));

            var nodeType = CreateNode(typeof(T));

            var rootField = Expression.Field(set, "root");

            var list = new List<Expression>
            {
                Expression.IfThen(Expression.Equal(rootField, Expression.Constant(null)),
                    Expression.Block(
                        Expression.Assign(rootField, Expression.New(nodeType.GetConstructor(new[] { typeof(T), typeof(bool) }), item, Expression.Constant(false, typeof(bool)))),
                        Expression.Assign(Expression.Field(set, "count"), Expression.Constant(1)),
                        Expression.AddAssign(Expression.Field(set, "version"), Expression.Constant(1)),
                        Expression.Return(exitPoint, Expression.Constant(false))))
            };

            var root = Expression.Variable(nodeType, "root");
            var node = Expression.Variable(nodeType, "node");
            var grandParent = Expression.Variable(nodeType, "grandParent");
            var greatGrandParent = Expression.Variable(nodeType, "greatGrandParent");
            var cmp = Expression.Variable(typeof(int), "cmp");

            list.Add(Expression.Assign(root, rootField));
            list.Add(Expression.Assign(node, Expression.Constant(null, nodeType)));
            list.Add(Expression.Assign(grandParent, Expression.Constant(null, nodeType)));
            list.Add(Expression.Assign(greatGrandParent, Expression.Constant(null, nodeType)));
            list.Add(Expression.AddAssign(Expression.Field(set, "version"), Expression.Constant(1)));
            list.Add(Expression.Assign(cmp, Expression.Constant(0)));

            var comparer = Expression.Variable(typeof(IComparer<T>), "comparer");
            list.Add(Expression.Assign(comparer, Expression.Field(set, "comparer")));

            var sortedSetType = typeof(SortedSet<T>);

            MethodInfo is4NodeMethod;
            MethodInfo split4NodeMethod;
            MethodInfo isRedMethod;
            MethodInfo insertionBalanceMethod;

#if NETFX_CORE
            is4NodeMethod = sortedSetType.GetMethod("Is4Node");
            split4NodeMethod = sortedSetType.GetMethod("Split4Node");
            isRedMethod = sortedSetType.GetMethod("IsRed");
            insertionBalanceMethod = sortedSetType.GetMethod("InsertionBalance");
#else
            is4NodeMethod = sortedSetType.GetMethod("Is4Node", BindingFlags.NonPublic | BindingFlags.Static);
            split4NodeMethod = sortedSetType.GetMethod("Split4Node", BindingFlags.NonPublic | BindingFlags.Static);
            isRedMethod = sortedSetType.GetMethod("IsRed", BindingFlags.NonPublic | BindingFlags.Static);
            insertionBalanceMethod = sortedSetType.GetMethod("InsertionBalance", BindingFlags.NonPublic | BindingFlags.Instance);
#endif

            var loopBody = Expression.Block(
                    Expression.Assign(cmp, Expression.Call(comparer, typeof(IComparer<T>).GetMethod("Compare", new[] { typeof(T), typeof(T) }), item, Expression.Field(root, "item"))),
                    Expression.IfThen(Expression.Equal(cmp, Expression.Constant(0)),
                        Expression.Block(
                            Expression.Assign(Expression.Field(rootField, "IsRed"), Expression.Constant(false)),
                            Expression.IfThenElse(Expression.NotEqual(onExist, Expression.Constant(null)),
                                        Expression.Assign(Expression.Field(root, "Item"), Expression.Call(onExist, onExist.Type.GetMethod("Invoke"), Expression.Field(root, "Item"), item)),
                                        Expression.Assign(Expression.Field(root, "Item"), item)
                                        ),
                            Expression.Return(exitPoint, Expression.Constant(true))
                                    )
                                ),

                    Expression.IfThen(Expression.Call(is4NodeMethod, root),
                                  Expression.Block(
                                            Expression.Call(split4NodeMethod, root),
                                            Expression.IfThen(Expression.Call(isRedMethod, node),
                                                Expression.Call(set, insertionBalanceMethod, root, node, grandParent, greatGrandParent)
                                        )
                                    )
                                ),
                    Expression.Assign(greatGrandParent, grandParent),
                    Expression.Assign(grandParent, node),
                    Expression.Assign(node, root),
                    Expression.IfThenElse(Expression.LessThan(cmp, Expression.Constant(0)),
                               Expression.Assign(root, Expression.Field(root, "Left")),
                               Expression.Assign(root, Expression.Field(root, "Right"))
                               )
                        );

            var afterLoop = Expression.Label("AfterLoop");

            list.Add(Expression.Loop(Expression.IfThenElse(Expression.NotEqual(root, Expression.Constant(null, nodeType)),
                    loopBody,
                    Expression.Break(afterLoop)), afterLoop)
                );

            var current = Expression.Variable(nodeType, "current");

            list.Add(Expression.Assign(current, Expression.New(nodeType.GetConstructor(new[] { typeof(T) }), item)));

            list.Add(Expression.IfThenElse(Expression.GreaterThan(cmp, Expression.Constant(0)),
                            Expression.Assign(Expression.Field(node, "Right"), current),
                            Expression.Assign(Expression.Field(node, "Left"), current)
                            )
                     );

            list.Add(Expression.IfThen(Expression.Field(node, "IsRed"),
                Expression.Call(set, insertionBalanceMethod, current, node, grandParent, greatGrandParent)
                ));

            list.Add(Expression.Assign(Expression.Field(rootField, "IsRed"), Expression.Constant(false)));
            list.Add(Expression.AddAssign(Expression.Field(set, "count"), Expression.Constant(1)));
            list.Add(Expression.Label(exitPoint, Expression.Constant(false)));

            var body = Expression.Block(new[] { comparer, root, node, grandParent, greatGrandParent, cmp, current }, list);

            var lambda = Expression.Lambda<Func<SortedSet<T>, T, Func<T, T, T>, bool>>(body, new[] { set, item, onExist });

            //public bool Replace(SortedSet<T> set, T item, Func<T, T, T> onExist)
            //{
            //    if (set.root == null)
            //    {
            //        set.root = new Node(item, false);
            //        set.count = 1;
            //        set.version++;
            //        return false;
            //    }

            //    SortedSet<T>.Node root = set.root;
            //    SortedSet<T>.Node node = null;
            //    SortedSet<T>.Node grandParent = null;
            //    SortedSet<T>.Node greatGrandParent = null;
            //    set.version++;
            //    int cmp = 0;
            //    var comparer = set.comparer;

            //    while (root != null)
            //    {
            //        cmp = comparer.Compare(item, root.Item);
            //        if (cmp == 0)
            //        {
            //            set.root.IsRed = false;
            //            root.Item = onExist != null ? onExist(root.Item, item) : item;
            //            return true;
            //        }

            //        if (SortedSet<T>.Is4Node(root))
            //        {
            //            SortedSet<T>.Split4Node(root);
            //            if (SortedSet<T>.IsRed(node))
            //                set.InsertionBalance(root, ref node, grandParent, greatGrandParent);
            //        }

            //        greatGrandParent = grandParent;
            //        grandParent = node;
            //        node = root;
            //        root = (cmp < 0) ? root.Left : root.Right;
            //    }

            //    SortedSet<T>.Node current = new SortedSet<T>.Node(item);
            //    if (cmp > 0)
            //        node.Right = current;
            //    else
            //        node.Left = current;

            //    if (node.IsRed)
            //        set.InsertionBalance(current, ref node, grandParent, greatGrandParent);

            //    set.root.IsRed = false;
            //    set.count++;
            //    return false;
            //}

            return lambda;
        }

        public Expression<Func<SortedSet<T>, T, T, bool, bool, SortedSet<T>>> CreateGetViewBetweenMethod()
        {
            var type = typeof(SortedSet<T>);
            Type treeSubSetType;

#if NETFX_CORE
            treeSubSetType = type.GetNestedType("TreeSubSet").MakeGenericType(typeof(T));
#else
            treeSubSetType = type.GetNestedType("TreeSubSet", BindingFlags.NonPublic).MakeGenericType(typeof(T));
#endif

            var set = Expression.Parameter(typeof(SortedSet<T>), "set");
            var lowerValue = Expression.Parameter(typeof(T), "lowerValue");
            var upperValue = Expression.Parameter(typeof(T), "upperValue");
            var lowerBoundActive = Expression.Parameter(typeof(bool), "lowerBoundActive");
            var upperBoundActive = Expression.Parameter(typeof(bool), "upperBoundActive");

            var hasFromAndHasTo = Expression.AndAlso(lowerBoundActive, upperBoundActive);

            var comparer = Expression.Property(set, type.GetProperty("Comparer"));
            var method = type.GetProperty("Comparer").PropertyType.GetMethod("Compare", new[] { typeof(T), typeof(T) });
            var call = Expression.Call(comparer, method, lowerValue, upperValue);

            var message = Expression.Constant("lowerBound is greater than upperBound", typeof(string));
            var exception = Expression.New(typeof(ArgumentException).GetConstructor(new[] { typeof(string) }), message);
            var @if = Expression.IfThen(Expression.AndAlso(hasFromAndHasTo, Expression.GreaterThan(call, Expression.Constant(0))),
                Expression.Throw(exception));

            var treeSubSetConstructor = treeSubSetType.GetConstructor(new[] { typeof(SortedSet<T>), typeof(T), typeof(T), typeof(bool), typeof(bool) });
            var result = Expression.New(treeSubSetConstructor, set, lowerValue, upperValue, lowerBoundActive, upperBoundActive);

            var body = Expression.Block(typeof(SortedSet<T>), @if, result);

            var lambda = Expression.Lambda<Func<SortedSet<T>, T, T, bool, bool, SortedSet<T>>>(body, set, lowerValue, upperValue, lowerBoundActive, upperBoundActive);

            //public SortedSet<T> GetViewBetween(SortedSet<T> set, T lowerValue, T upperValue, bool lowerBoundActive, bool upperBoundActive)
            //{
            //   if (lowerBoundActive && upperBoundActive) && set.Comparer.Compare(lowerValue, upperValue) > 0)
            //        throw new ArgumentException("lowerBound is greater than upperBound");

            //    return new TreeSubSet(set, lowerValue, upperValue, lowerBoundActive, lowerBoundActive);
            //}

            return lambda;
        }

        public Expression<Func<SortedSet<T>, T, KeyValuePair<bool, T>>> CreateFindNextMethod()
        {
            var set = Expression.Parameter(typeof(SortedSet<T>), "set");
            var item = Expression.Parameter(typeof(T), "item");
            var cmp = Expression.Variable(typeof(int), "cmp");

            var nodeType = CreateNode(typeof(T));
            var next = Expression.Variable(nodeType, "next");
            var node = Expression.Variable(nodeType, "node");

            var returnLabel = Expression.Label(typeof(KeyValuePair<bool, T>));

            var list = new List<Expression>
            {
                Expression.Assign(next, Expression.Constant(null, nodeType)),
                Expression.Assign(node, Expression.Field(set, "root"))
            };

            var nodeItem = Expression.PropertyOrField(node, "Item");
            var nodeLeft = Expression.PropertyOrField(node, "Left");
            var nodeRight = Expression.PropertyOrField(node, "Right");

            var afterLoop = Expression.Label("AfterLoop");

            list.Add(Expression.Loop(Expression.IfThenElse(Expression.NotEqual(node, Expression.Constant(null)),
                 Expression.Block(Expression.Assign(cmp,
                     Expression.Call(Expression.PropertyOrField(set, "comparer"), typeof(IComparer<T>).GetMethod("Compare"), item,
                     nodeItem)),
                 Expression.IfThen(Expression.Equal(cmp, Expression.Constant(0, typeof(int))),
                     Expression.Return(returnLabel,
                     Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }), Expression.Constant(true, typeof(bool)), nodeItem))),
                 Expression.IfThenElse(Expression.LessThan(cmp, Expression.Constant(0, typeof(int))),
                      Expression.Block(Expression.Assign(next, node),
                         Expression.Assign(node, nodeLeft)),
                      Expression.Assign(node, nodeRight))), Expression.Break(afterLoop)), afterLoop));

            var nextItem = Expression.PropertyOrField(next, "Item");

            list.Add(Expression.IfThen(Expression.NotEqual(next, Expression.Constant(null)),
                Expression.Return(returnLabel,
                     Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }), Expression.Constant(true, typeof(bool)), nextItem))));

            list.Add(Expression.Label(returnLabel,
                Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }),
                Expression.Constant(false, typeof(bool)), Expression.Constant(default(T), typeof(T)))));

            //public KeyValuePair<bool, T> FindNext(SortedSet<T> set, T item)
            //{
            //    int cmp;
            //    Node next = null;
            //    Node node = set.root;

            //    while (node != null)
            //    {
            //        cmp = set.comparer.Compare(item, node.Item);

            //        if (cmp == 0)
            //            return new KeyValuePair<bool,T>(true, node.Item);

            //        if (cmp < 0)
            //        {
            //            next = node;
            //            node = node.Left;
            //        }
            //        else
            //            node = node.Right;
            //    }

            //    if (next != null)
            //        return new KeyValuePair<bool, T>(true, next.Item);

            //    return new KeyValuePair<bool, T>(false, default(T));
            //}

            return Expression.Lambda<Func<SortedSet<T>, T, KeyValuePair<bool, T>>>(Expression.Block(new[] { cmp, next, node }, list), set, item);
        }

        public Expression<Func<SortedSet<T>, T, KeyValuePair<bool, T>>> CreateFindPrevMethod()
        {
            var set = Expression.Parameter(typeof(SortedSet<T>), "set");
            var item = Expression.Parameter(typeof(T), "item");
            var cmp = Expression.Variable(typeof(int), "cmp");

            var nodeType = CreateNode(typeof(T));
            var prev = Expression.Variable(nodeType, "prev");
            var node = Expression.Variable(nodeType, "node");

            var returnLabel = Expression.Label(typeof(KeyValuePair<bool, T>));

            var list = new List<Expression>
            {
                Expression.Assign(prev, Expression.Constant(null, nodeType)),
                Expression.Assign(node, Expression.Field(set, "root"))
            };

            var nodeItem = Expression.PropertyOrField(node, "Item");
            var nodeLeft = Expression.PropertyOrField(node, "Left");
            var nodeRight = Expression.PropertyOrField(node, "Right");

            var afterLoop = Expression.Label("AfterLoop");

            list.Add(Expression.Loop(Expression.IfThenElse(Expression.NotEqual(node, Expression.Constant(null)),
                 Expression.Block(Expression.Assign(cmp,
                     Expression.Call(Expression.PropertyOrField(set, "comparer"), typeof(IComparer<T>).GetMethod("Compare"), item,
                     nodeItem)),
                 Expression.IfThen(Expression.Equal(cmp, Expression.Constant(0, typeof(int))),
                     Expression.Return(returnLabel,
                     Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }), Expression.Constant(true, typeof(bool)), nodeItem))),
                 Expression.IfThenElse(Expression.GreaterThan(cmp, Expression.Constant(0, typeof(int))),
                      Expression.Block(Expression.Assign(prev, node),
                         Expression.Assign(node, nodeRight)),
                      Expression.Assign(node, nodeLeft))), Expression.Break(afterLoop)), afterLoop));

            var prevItem = Expression.PropertyOrField(prev, "Item");

            list.Add(Expression.IfThen(Expression.NotEqual(prev, Expression.Constant(null)),
                Expression.Return(returnLabel,
                     Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }), Expression.Constant(true, typeof(bool)), prevItem))));

            list.Add(Expression.Label(returnLabel,
                Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }),
                Expression.Constant(false, typeof(bool)), Expression.Constant(default(T), typeof(T)))));

            //public KeyValuePair<bool, T> FindPrev(SortedSet<T> set, T item)
            //{
            //    int cmp;
            //    Node prev = null;
            //    Node node = set.root;

            //    while (node != null)
            //    {
            //        cmp = set.comparer.Compare(item, node.Item);

            //        if (cmp == 0)
            //            return new KeyValuePair<bool,T>(true, node.Item);

            //        if (cmp > 0)
            //        {
            //            prev = node;
            //            node = node.Right;
            //        }
            //        else
            //            node = node.Left;
            //    }

            //    if (prev != null)
            //        return new KeyValuePair<bool, T>(true, prev.Item);

            //    return new KeyValuePair<bool, T>(false, default(T));
            //}

            return Expression.Lambda<Func<SortedSet<T>, T, KeyValuePair<bool, T>>>(Expression.Block(new[] { cmp, prev, node }, list), set, item);
        }

        public Expression<Func<SortedSet<T>, T, KeyValuePair<bool, T>>> CreateFindAfterMethod()
        {
            var set = Expression.Parameter(typeof(SortedSet<T>), "set");
            var item = Expression.Parameter(typeof(T), "item");
            var cmp = Expression.Variable(typeof(int), "cmp");

            var nodeType = CreateNode(typeof(T));
            var after = Expression.Variable(nodeType, "after");
            var node = Expression.Variable(nodeType, "node");
            var tmp = Expression.Variable(nodeType, "tmp");

            var returnLabel = Expression.Label(typeof(KeyValuePair<bool, T>));

            var list = new List<Expression>
            {
                Expression.Assign(after, Expression.Constant(null, nodeType)),
                Expression.Assign(node, Expression.Field(set, "root"))
            };

            var nodeItem = Expression.PropertyOrField(node, "Item");
            var nodeLeft = Expression.PropertyOrField(node, "Left");
            var nodeRight = Expression.PropertyOrField(node, "Right");

            var afterLoop = Expression.Label("AfterLoop");

            list.Add(Expression.Loop(Expression.IfThenElse(Expression.NotEqual(node, Expression.Constant(null)),
                 Expression.Block(Expression.Assign(cmp,
                     Expression.Call(Expression.PropertyOrField(set, "comparer"), typeof(IComparer<T>).GetMethod("Compare"), item,
                     nodeItem)),
                 Expression.IfThen(Expression.Equal(cmp, Expression.Constant(0, typeof(int))),
                    Expression.Block(Expression.IfThen(Expression.NotEqual(nodeRight, Expression.Constant(null, nodeType)),
                            Expression.Block(new[] { tmp }, Expression.Assign(tmp, nodeRight),
                                Expression.Loop(Expression.IfThenElse(Expression.NotEqual(tmp, Expression.Constant(null, nodeType)),
                                    Expression.Block(Expression.Assign(after, tmp),
                                        Expression.Assign(tmp, Expression.PropertyOrField(tmp, "Left"))), Expression.Break(afterLoop))))),
                        Expression.Break(afterLoop))),
                 Expression.IfThenElse(Expression.LessThan(cmp, Expression.Constant(0, typeof(int))),
                      Expression.Block(Expression.Assign(after, node),
                         Expression.Assign(node, nodeLeft)),
                      Expression.Assign(node, nodeRight))), Expression.Break(afterLoop)), afterLoop));

            var afterItem = Expression.PropertyOrField(after, "Item");

            list.Add(Expression.IfThen(Expression.NotEqual(after, Expression.Constant(null)),
                Expression.Return(returnLabel,
                     Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }), Expression.Constant(true, typeof(bool)), afterItem))));

            list.Add(Expression.Label(returnLabel,
                Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }),
                Expression.Constant(false, typeof(bool)), Expression.Constant(default(T), typeof(T)))));

            //public KeyValuePair<bool,T> FindAfter(SortedSet<T> set, T item)
            //{
            //    int cmp;
            //    Node next = null;
            //    Node node = set.root

            //    while(node != null)
            //    {
            //        cmp = set.comparer.Compare(item, node.Item);

            //        if (cmp == 0)
            //        {
            //            if (node.Right != null)
            //            {
            //                Node tmp = node.Right;

            //                while (tmp != null)
            //                {
            //                    next = tmp;
            //                    tmp = tmp.Left;
            //                }
            //            }

            //            break;
            //        }

            //        if (cmp < 0)
            //        {
            //            next = node;
            //            node = node.Left;
            //        }
            //        else
            //            node = node.Right;
            //    }

            //    if (next != null)
            //        return new KeyValuePair<bool,T>(true, next.Item);

            //    return new KeyValuePair<bool,T>(false, default(T));
            //}

            return Expression.Lambda<Func<SortedSet<T>, T, KeyValuePair<bool, T>>>(Expression.Block(new[] { cmp, after, node }, list), set, item);
        }

        public Expression<Func<SortedSet<T>, T, KeyValuePair<bool, T>>> CreateFindBeforeMethod()
        {
            var set = Expression.Parameter(typeof(SortedSet<T>), "set");
            var item = Expression.Parameter(typeof(T), "item");
            var cmp = Expression.Variable(typeof(int), "cmp");

            var nodeType = CreateNode(typeof(T));
            var before = Expression.Variable(nodeType, "before");
            var node = Expression.Variable(nodeType, "node");
            var tmp = Expression.Variable(nodeType, "tmp");

            var returnLabel = Expression.Label(typeof(KeyValuePair<bool, T>));

            var list = new List<Expression>
            {
                Expression.Assign(before, Expression.Constant(null, nodeType)),
                Expression.Assign(node, Expression.Field(set, "root"))
            };

            var nodeItem = Expression.PropertyOrField(node, "Item");
            var nodeLeft = Expression.PropertyOrField(node, "Left");
            var nodeRight = Expression.PropertyOrField(node, "Right");

            var afterLoop = Expression.Label("AfterLoop");

            list.Add(Expression.Loop(Expression.IfThenElse(Expression.NotEqual(node, Expression.Constant(null)),
                 Expression.Block(Expression.Assign(cmp,
                     Expression.Call(Expression.PropertyOrField(set, "comparer"), typeof(IComparer<T>).GetMethod("Compare"), item,
                     nodeItem)),
                 Expression.IfThen(Expression.Equal(cmp, Expression.Constant(0, typeof(int))),
                    Expression.Block(Expression.IfThen(Expression.NotEqual(nodeLeft, Expression.Constant(null, nodeType)),
                            Expression.Block(new[] { tmp }, Expression.Assign(tmp, nodeLeft),
                                Expression.Loop(Expression.IfThenElse(Expression.NotEqual(tmp, Expression.Constant(null, nodeType)),
                                    Expression.Block(Expression.Assign(before, tmp),
                                        Expression.Assign(tmp, Expression.PropertyOrField(tmp, "Right"))), Expression.Break(afterLoop))))),
                        Expression.Break(afterLoop))),
                 Expression.IfThenElse(Expression.GreaterThan(cmp, Expression.Constant(0, typeof(int))),
                      Expression.Block(Expression.Assign(before, node),
                         Expression.Assign(node, nodeRight)),
                      Expression.Assign(node, nodeLeft))), Expression.Break(afterLoop)), afterLoop));

            var beforeItem = Expression.PropertyOrField(before, "Item");

            list.Add(Expression.IfThen(Expression.NotEqual(before, Expression.Constant(null)),
                Expression.Return(returnLabel,
                     Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }), Expression.Constant(true, typeof(bool)), beforeItem))));

            list.Add(Expression.Label(returnLabel,
                Expression.New(typeof(KeyValuePair<bool, T>).GetConstructor(new[] { typeof(bool), typeof(T) }),
                Expression.Constant(false, typeof(bool)), Expression.Constant(default(T), typeof(T)))));

            //public KeyValuePair<bool,T> FindBefore(SortedSet<T> set, T item)
            //{
            //    int cmp;
            //    Node prev = null;
            //    Node node = set.root

            //    while(node != null)
            //    {
            //        cmp = set.comparer.Compare(item, node.Item);

            //        if (cmp == 0)
            //        {
            //            if (node.Left != null)
            //            {
            //                Node tmp = node.Left;

            //                while (tmp != null)
            //                {
            //                    prev = tmp;
            //                    tmp = tmp.Right;
            //                }
            //            }

            //            break;
            //        }

            //        if (cmp > 0)
            //        {
            //            prev = node;
            //            node = node.Right;
            //        }
            //        else
            //            node = node.Left;
            //    }

            //    if (prev != null)
            //        return new KeyValuePair<bool,T>(true, prev.Item);

            //    return new KeyValuePair<bool,T>(false, default(T));
            //}

            return Expression.Lambda<Func<SortedSet<T>, T, KeyValuePair<bool, T>>>(Expression.Block(new[] { cmp, before, node }, list), set, item);
        }

        private static Type CreateNode(Type genericType)
        {
#if NETFX_CORE
            Type type = typeof(SortedSet<T>);
            Type nodeType = type.GetNestedType("Node").MakeGenericType(genericType);

            return nodeType;
#else
            var type = typeof(SortedSet<T>);
            var nodeType = type.GetNestedType("Node", BindingFlags.NonPublic).MakeGenericType(genericType);

            return nodeType;
#endif
        }

        public bool TryGetValue(SortedSet<T> set, T key, out T value)
        {
            var kv = _find(set, key);

            if (!kv.Key)
            {
                value = default(T);
                return false;
            }

            value = kv.Value;
            return true;
        }

        public void ConstructFromSortedArray(SortedSet<T> set, T[] array, int index, int count)
        {
            _constructFromSortedArray(set, array, index, count);
        }

        public bool Replace(SortedSet<T> set, T item, Func<T, T, T> onExist)
        {
            return _replace(set, item, onExist);
        }

        public SortedSet<T> GetViewBetween(SortedSet<T> set, T lowerValue, T upperValue, bool lowerBoundActive, bool upperBoundActive)
        {
            return _getViewBetween(set, lowerValue, upperValue, lowerBoundActive, upperBoundActive);
        }

        public KeyValuePair<bool, T> FindNext(SortedSet<T> set, T key)
        {
            return _findNext(set, key);
        }

        public KeyValuePair<bool, T> FindPrev(SortedSet<T> set, T key)
        {
            return _findPrev(set, key);
        }

        public KeyValuePair<bool, T> FindAfter(SortedSet<T> set, T key)
        {
            return _findAfter(set, key);
        }

        public KeyValuePair<bool, T> FindBefore(SortedSet<T> set, T key)
        {
            return _findBefore(set, key);
        }
    }

    public static class SortedSetExtensions
    {
        public static bool TryGetValue<T>(this SortedSet<T> set, T key, out T value)
        {
            return SortedSetHelper<T>.Instance.TryGetValue(set, key, out value);
        }

        /// <summary>
        /// Elements in array must be ordered and unique from the set.Comparer point of view.
        /// </summary>
        public static void ConstructFromSortedArray<T>(this SortedSet<T> set, T[] array, int index, int count)
        {
            SortedSetHelper<T>.Instance.ConstructFromSortedArray(set, array, index, count);
        }

        /// <summary>
        /// Replace an existing element.
        /// 1.If item not exists it will be added to the set.
        /// 2.If item already exist there is two cases:
        ///  - if onExists is null the new item will replace existing item;
        ///  - if onExists is not null the item returned by the onExist(T existingItem, T newItem) function will replace the existing item.
        /// </summary>
        /// <typeparam name="T">The type of the elements in the set.</typeparam>
        /// <param name="set"></param>
        /// <param name="item">The element to add in the set</param>
        /// <param name="onExist">T onExist(T existingItem, T newItem)</param>
        /// <returns>
        /// Returns false if the specified item not exist in the set (insert).
        /// Returns true if the specified item exist in the set (replace).
        /// </returns>
        public static bool Replace<T>(this SortedSet<T> set, T item, Func<T, T, T> onExist)
        {
            return SortedSetHelper<T>.Instance.Replace(set, item, onExist);
        }

        /// <summary>
        /// Replace an existing element.
        /// 1.If item not exists it will be added to the set.
        /// 2.If item already exist the new item will replace existing item;
        /// </summary>
        /// <typeparam name="T">The type of the elements in the set.</typeparam>
        /// <param name="set"></param>
        /// <param name="item">The element to add in the set</param>
        /// <returns>
        /// Returns false if the specified item not exist in the set (insert).
        /// Returns true if the specified item exist in the set (replace).
        /// </returns>
        public static bool Replace<T>(this SortedSet<T> set, T item)
        {
            return set.Replace(item, null);
        }

        public static SortedSet<T> GetViewBetween<T>(this SortedSet<T> set, T lowerValue, T upperValue, bool lowerBoundActive, bool upperBoundActive)
        {
            return SortedSetHelper<T>.Instance.GetViewBetween(set, lowerValue, upperValue, lowerBoundActive, upperBoundActive);
        }

        public static bool FindNext<T>(this SortedSet<T> set, T key, out T value)
        {
            var returnValue = SortedSetHelper<T>.Instance.FindNext(set, key);

            value = returnValue.Value;

            return returnValue.Key;
        }

        public static bool FindPrev<T>(this SortedSet<T> set, T key, out T value)
        {
            var returnValue = SortedSetHelper<T>.Instance.FindPrev(set, key);

            value = returnValue.Value;

            return returnValue.Key;
        }

        public static bool FindAfter<T>(this SortedSet<T> set, T key, out T value)
        {
            var returnValue = SortedSetHelper<T>.Instance.FindAfter(set, key);

            value = returnValue.Value;

            return returnValue.Key;
        }

        public static bool FindBefore<T>(this SortedSet<T> set, T key, out T value)
        {
            var returnValue = SortedSetHelper<T>.Instance.FindBefore(set, key);

            value = returnValue.Value;

            return returnValue.Key;
        }

        /// <summary>
        /// Splits the set into two parts, where the right part contains count number of elements and return the right part of the set.
        /// </summary>
        public static SortedSet<T> Split<T>(this SortedSet<T> set, int count)
        {
            var array = new T[set.Count];
            set.CopyTo(array);

            Debug.Assert(array.IsOrdered(set.Comparer, true));

            set.ConstructFromSortedArray(array, 0, array.Length - count);

            Debug.Assert(set.IsOrdered(set.Comparer, true));

            var right = new SortedSet<T>(set.Comparer);
            right.ConstructFromSortedArray(array, array.Length - count, count);

            Debug.Assert(right.IsOrdered(right.Comparer, true));

            return right;
        }

        public static bool Remove<T>(this SortedSet<T> set, T fromKey, T toKey)
        {
            var cmp = set.Comparer.Compare(fromKey, toKey);
            if (cmp > 0)
                throw new ArgumentException();

            if (cmp == 0)
                return set.Remove(fromKey);

            var arr = new T[set.Count];
            set.CopyTo(arr);

            var from = Array.BinarySearch(arr, fromKey, set.Comparer);
            var fromIdx = from;
            if (fromIdx < 0)
            {
                fromIdx = ~fromIdx;
                if (fromIdx == set.Count)
                    return false;
            }

            var to = Array.BinarySearch(arr, fromIdx, set.Count - fromIdx, toKey, set.Comparer);
            var toIdx = to;
            if (toIdx < 0)
            {
                if (from == to)
                    return false;

                toIdx = ~toIdx - 1;
            }

            var count = toIdx - fromIdx + 1;
            if (count == 0)
                return false;

            Array.Copy(arr, toIdx + 1, arr, fromIdx, set.Count - (toIdx + 1));

            Debug.Assert(arr.Take(set.Count - count).IsOrdered(set.Comparer, true));

            set.ConstructFromSortedArray(arr, 0, set.Count - count);

            return true;
        }
    }
}
