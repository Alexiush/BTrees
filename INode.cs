namespace HeapsAndBTrees
{
    public interface INode<TKey>
        where TKey : IComparable
    {
        public int KeysCount { get; protected set; }
        protected ref TKey[] Keys { get; }

        public TKey GetKey(int index) => Keys[index];
        public void SetKey(int index, TKey key) => Keys[index] = key;
    }

    public interface IValueNode<TKey, TValue> : INode<TKey>
        where TKey : IComparable
    {
        protected ref TValue[] Values { get; }

        public TValue GetValue(int index) => Values[index];
        public void SetValue(int index, TValue key) => Values[index] = key;

        public void InsertValue(int index, TKey key, TValue value)
        {
            if (index < KeysCount)
            {
                Utilities.ShiftArrayRight(ref Keys, index, KeysCount);
                Utilities.ShiftArrayRight(ref Values, index, KeysCount);
            }

            Keys[index] = key;
            Values[index] = value;
            KeysCount++;
        }

        public void RemoveValue(int index)
        {
            Keys[index] = default;
            Values[index] = default;

            if (index != KeysCount - 1)
            {
                Utilities.ShiftArrayLeft(ref Keys, index, KeysCount - 1);
                Utilities.ShiftArrayLeft(ref Values, index, KeysCount - 1);
            }
            KeysCount--;
        }
    }

    public interface IInternalNode<T, TKey> : INode<TKey>
        where T : INode<TKey>
        where TKey : IComparable
    {
        protected ref T[] Children { get; }

        public T GetChild(int index)
        {
            return Children[index];
        }

        public void SetChild(int index, T child)
        {
            Children[index] = child;
        }

        public void InsertChild(int index, T child)
        {
            if (index < KeysCount)
            {
                Utilities.ShiftArrayRight(ref Children, index, KeysCount);
            }

            SetChild(index, child);
        }

        public void RemoveChild(int index)
        {
            Children[index] = default;
            Utilities.ShiftArrayLeft(ref Children, index, Keys.Length);
        }
    }
}
