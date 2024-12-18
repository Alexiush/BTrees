using Optional;

namespace HeapsAndBTrees
{
    public class BStarTree<TKey, TValue> :
        DiagnostableBTreeBase<
            TKey, TValue,
            BStarTree<TKey, TValue>.IBStarTreeNode,
            BStarTree<TKey, TValue>.VirtualNode,
            BStarTree<TKey, TValue>.Node
        >
        where TKey : IComparable
    {
        public interface IBStarTreeNode : INode<TKey>, IValueNode<TKey, TValue>, IInternalNode<IBStarTreeNode, TKey>
        {
            public void Expand(int newSize);
            public void Shrink(int newSize);

            public void InsertFull(int index, TKey key, IBStarTreeNode child, TValue value, bool leftToRight);
            public void RemoveFull(int index, bool leftToRight);

            public bool IsLeaf();

            public void Print(int offset = 0);
        }

        public class Node : IBStarTreeNode, IActualNode<IBStarTreeNode, TKey>
        {
            private IBStarTreeNode SelfTyped => this;

            protected int _keysCount;
            public int KeysCount { get { return _keysCount; } set { _keysCount = value; } }

            protected TKey[] _keys;
            public ref TKey[] Keys { get { return ref _keys; } }

            protected TValue[] _values;
            public ref TValue[] Values { get { return ref _values; } }

            protected IBStarTreeNode[] _children;
            public ref IBStarTreeNode[] Children { get { return ref _children; } }

            IBStarTreeNode IInternalNode<IBStarTreeNode, TKey>.GetChild(int index)
            {
                if (Children is null)
                {
                    InitializeChildren();
                }

                return Children[index];
            }

            void IInternalNode<IBStarTreeNode, TKey>.SetChild(int index, IBStarTreeNode child)
            {
                if (Children is null)
                {
                    InitializeChildren();
                }

                Node childTyped = child switch
                {
                    Node node => node,
                    VirtualNode virtualNode => virtualNode.Node,
                    _ => throw new ArgumentException("Could not retrieve an actual node")
                };

                Children[index] = childTyped;
            }

            protected void InitializeChildren()
            {
                int size = Keys.Length + 1;
                Children = new Node[size];
            }

            public void Expand(int newSize)
            {
                var newKeys = new TKey[newSize - 1];
                Array.Copy(Keys, newKeys, Keys.Length);
                Keys = newKeys;

                var newValues = new TValue[newSize - 1];
                Array.Copy(Values, newValues, Values.Length);
                Values = newValues;

                if (Children is not null)
                {
                    var newChildren = new Node[newSize];
                    Array.Copy(Children, newChildren, Children.Length);
                    Children = newChildren;
                }
            }

            public void Shrink(int newSize)
            {
                var newKeys = new TKey[newSize - 1];
                Array.Copy(Keys, newKeys, newKeys.Length);
                Keys = newKeys;

                var newValues = new TValue[newSize - 1];
                Array.Copy(Values, newValues, newValues.Length);
                Values = newValues;

                if (Children is not null)
                {
                    var newChildren = new Node[newSize];
                    Array.Copy(Children, newChildren, newChildren.Length);
                    Children = newChildren;
                }
            }

            void IInternalNode<IBStarTreeNode, TKey>.InsertChild(int index, IBStarTreeNode child)
            {
                if (Children is null)
                {
                    InitializeChildren();
                }

                if (index < KeysCount)
                {
                    Utilities.ShiftArrayRight(ref Children, index, KeysCount);
                }

                SelfTyped.SetChild(index, child);
            }

            public void InsertFull(int index, TKey key, IBStarTreeNode child, TValue value, bool leftToRight)
            {
                SelfTyped.InsertValue(index, key, value);
                SelfTyped.InsertChild(leftToRight ? index : index + 1, child);
            }

            public void RemoveFull(int index, bool leftToRight)
            {
                SelfTyped.RemoveValue(index);
                if (IsLeaf())
                {
                    return;
                }

                SelfTyped.RemoveChild(leftToRight ? index + 1 : index);
            }

            public Node(int size)
            {
                KeysCount = 0;
                Keys = new TKey[size - 1];
                Values = new TValue[size - 1];
            }

            public bool IsLeaf() => Children is null;

            public void Print(int offset = 0)
            {
                for (int i = 0; i < offset; i++)
                {
                    Console.Write("\t");
                }

                for (int i = 0; i < KeysCount; i++)
                {
                    Console.Write(Keys[i]);
                    Console.Write(" ");
                }
                Console.Write("_");
                Console.WriteLine();

                if (Children is null)
                {
                    return;
                }

                for (int i = 0; i <= KeysCount; i++)
                {
                    if (Children[i] is null)
                    {
                        Console.WriteLine("Something fishy");
                    }

                    Children[i].Print(offset + 1);
                }
            }
        }

        public class VirtualNode : IBStarTreeNode, IVirtualNode<Node, IBStarTreeNode, TKey>
        {
            public Node Node { get; protected set; }
            public IBStarTreeNode NodeTyped => Node;

            private BTreeContext<TKey, TValue, IBStarTreeNode, VirtualNode, Node> _context;

            public int KeysCount
            {
                get
                {
                    _context.Actualize(this);
                    return NodeTyped.KeysCount;
                }
                set
                {
                    throw new Exception("Virtual node should change keys count only via wrapped node");
                }
            }

            public ref TKey[] Keys
            {
                get
                {
                    _context.Actualize(this);
                    return ref Node.Keys;
                }
            }

            public ref TValue[] Values
            {
                get
                {
                    _context.Actualize(this);
                    return ref Node.Values;
                }
            }

            public ref IBStarTreeNode[] Children
            {
                get
                {
                    _context.Actualize(this);
                    return ref Node.Children;
                }
            }

            TKey INode<TKey>.GetKey(int index)
            {
                _context.Actualize(this);
                return NodeTyped.GetKey(index);
            }

            void INode<TKey>.SetKey(int index, TKey key)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.SetKey(index, key);
            }

            TValue IValueNode<TKey, TValue>.GetValue(int index)
            {
                _context.Actualize(this);
                return NodeTyped.GetValue(index);
            }

            void IValueNode<TKey, TValue>.SetValue(int index, TValue key)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.SetValue(index, key);
            }

            IBStarTreeNode IInternalNode<IBStarTreeNode, TKey>.GetChild(int index)
            {
                _context.Actualize(this);
                Node child = NodeTyped.GetChild(index) as Node;

                if (child is null)
                {
                    new ArgumentException("Requested child was null");
                }

                var virtualChild = new VirtualNode(child, _context);
                return virtualChild;
            }

            void IInternalNode<IBStarTreeNode, TKey>.SetChild(int index, IBStarTreeNode child)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.SetChild(index, child);
            }

            public bool IsDirty { get; protected set; } = false;

            public VirtualNode(Node node, BTreeContext<TKey, TValue, IBStarTreeNode, VirtualNode, Node> context)
            {
                Node = node;
                _context = context;
            }

            public void Expand(int newSize)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.Expand(newSize);
            }

            public void Shrink(int newSize)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.Shrink(newSize);
            }

            void IValueNode<TKey, TValue>.InsertValue(int index, TKey key, TValue value)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertValue(index, key, value);
            }

            void IInternalNode<IBStarTreeNode, TKey>.InsertChild(int index, IBStarTreeNode child)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertChild(index, child);
            }

            public void InsertFull(int index, TKey key, IBStarTreeNode child, TValue value, bool leftToRight)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertFull(index, key, child, value, leftToRight);
            }

            void IValueNode<TKey, TValue>.RemoveValue(int index)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.RemoveValue(index);
            }

            void IInternalNode<IBStarTreeNode, TKey>.RemoveChild(int index)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.RemoveChild(index);
            }

            public void RemoveFull(int index, bool leftToRight)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.RemoveFull(index, leftToRight);
            }

            public bool IsLeaf()
            {
                _context.Actualize(this);
                return NodeTyped.IsLeaf();
            }

            public void Print(int offset = 0)
            {
                _context.Actualize(this);
                NodeTyped.Print(offset);
            }
        }

        private int _size;
        private int RootSize => (2 * ((2 * _size - 2) / 3)) + 1;

        private Node _root;

        public BStarTree(int size)
        {
            _size = size;
            UpdateRoot(new Node(RootSize));
        }

        private BTreeContext<TKey, TValue, IBStarTreeNode, VirtualNode, Node> CreateContext(BTreeOperation caller) =>
            new BTreeContext<TKey, TValue, IBStarTreeNode, VirtualNode, Node>(
                this,
                (n, op) => DiskRead(n, op),
                (n, op) => DiskWrite(n, op),
                (n, context) => new VirtualNode(n, context),
                _root, caller, 4
            );

        public void UpdateRoot(IBStarTreeNode newRoot)
        {
            Node newRootTyped = newRoot switch
            {
                Node node => node,
                VirtualNode virtualNode => virtualNode.Node,
                _ => throw new ArgumentException("Could not retrieve an actual node")
            };

            _root = newRootTyped;
            InvokeOnRootChanged(newRootTyped);
        }

        public override void Clear()
        {
            _root = new Node(RootSize);
        }

        public void DiskWrite(IBStarTreeNode node, BTreeOperation caller)
        {
            if (!_watched)
            {
                return;
            }

            _diagnosticsData[caller].Writes++;
        }

        public void DiskRead(IBStarTreeNode node, BTreeOperation caller)
        {
            if (!_watched)
            {
                return;
            }

            _diagnosticsData[caller].Reads++;
        }

        private bool IsFull(IBStarTreeNode node) => (IsRoot(node) && node.KeysCount == (RootSize - 1)) || node.KeysCount == (_size - 1);
        private bool IsBig(IBStarTreeNode node) => node.KeysCount >= ((2 * _size - 1) / 3) - 1;
        private bool IsRoot(IBStarTreeNode node) => node switch
        {
            Node n => n == _root,
            VirtualNode v => v.Node == _root,
            _ => throw new ArgumentException("Could not retrieve an actual node")
        };

        private void Split(IBStarTreeNode parent, IBStarTreeNode child, int childIndex)
        {
            IBStarTreeNode node = new Node(_size);
            int size = child.KeysCount + 1;
            int median = (size / 2) - 1;

            if (!child.IsLeaf())
            {
                node.InsertChild(0, child.GetChild(size - 1));
                child.RemoveChild(size - 1);
            }

            parent.InsertFull(childIndex, child.GetKey(_size - 2), node, child.GetValue(_size - 2), false);
            child.RemoveValue(_size - 2);

            for (int index = size - 3; index >= median; index--)
            {
                RightTransfer(child, node, parent, childIndex);
            }
        }

        private void TwoThreeSplit(IBStarTreeNode parent, IBStarTreeNode child, IBStarTreeNode sibling, int childIndex)
        {
            IBStarTreeNode node = new Node(_size);
            int median = (_size / 3) - 1;

            if (!sibling.IsLeaf())
            {
                node.InsertChild(0, sibling.GetChild(sibling.KeysCount));
                sibling.RemoveChild(sibling.KeysCount);
            }

            parent.InsertFull(childIndex + 1, sibling.GetKey(sibling.KeysCount - 1), node, sibling.GetValue(sibling.KeysCount - 1), false);
            sibling.RemoveValue(sibling.KeysCount - 1);

            for (int index = sibling.KeysCount - 1; index > median; index--)
            {
                RightTransfer(sibling, node, parent, childIndex + 1);
            }

            median = (2 * _size / 3) - 1;
            while (child.KeysCount > median)
            {
                RightTransfer(child, sibling, parent, childIndex);
            }
        }

        private void Transfer(IBStarTreeNode donor, IBStarTreeNode donee, IBStarTreeNode parent, int mutualKey, bool leftToRight)
        {
            int insertIndex = leftToRight ? 0 : donee.KeysCount;
            int transferIndex = leftToRight ? donor.KeysCount - 1 : 0;
            int childTransferIndex = leftToRight ? transferIndex + 1 : transferIndex;

            TKey transferKeyTemp = donor.GetKey(transferIndex);
            TValue valueTemp = donor.GetValue(transferIndex);

            TKey transferKey = parent.GetKey(mutualKey);
            TValue value = parent.GetValue(mutualKey);

            parent.SetKey(mutualKey, transferKeyTemp);
            parent.SetValue(mutualKey, valueTemp);

            if (donor.IsLeaf())
            {
                donee.InsertValue(insertIndex, transferKey, value);
            }
            else
            {
                donee.InsertFull(insertIndex, transferKey, donor.GetChild(childTransferIndex), value, leftToRight);
            }
            donor.RemoveFull(transferIndex, leftToRight);
        }

        private void LeftTransfer(IBStarTreeNode donor, IBStarTreeNode donee, IBStarTreeNode parent, int mutualKey) => Transfer(donor, donee, parent, mutualKey, false);
        private void RightTransfer(IBStarTreeNode donor, IBStarTreeNode donee, IBStarTreeNode parent, int mutualKey) => Transfer(donor, donee, parent, mutualKey, true);

        private int FindIndex(IBStarTreeNode pointer, TKey key)
        {
            int start = 0;
            int end = pointer.KeysCount;

            while (start < end)
            {
                int middle = (start + end) / 2;

                int comparisonResult = key.CompareTo(pointer.GetKey(middle));

                switch (comparisonResult)
                {
                    case 0:
                        return middle;
                    case < 0:
                        end = middle;
                        break;
                    default:
                        start = middle + 1;
                        break;
                }
            }

            return end;
        }

        private void IndexFixup(TKey key, IBStarTreeNode pointer, ref int index)
        {
            while (index < pointer.KeysCount && key.CompareTo(pointer.GetKey(index)) > 0)
            {
                index++;
            }
        }

        private void Distribute(IBStarTreeNode donor, IBStarTreeNode donee, IBStarTreeNode parent, int index, bool leftToRight)
        {
            int totalKeys = donor.KeysCount + donee.KeysCount;
            int childKeys = totalKeys - (totalKeys / 2);

            while (donee.KeysCount != childKeys)
            {
                Transfer(donor, donee, parent, index, leftToRight);
            }
        }

        private bool TryDistributeAndInsert(IBStarTreeNode donor, IBStarTreeNode donee, IBStarTreeNode parent, TKey key, TValue value, ref int index, bool leftToRight)
        {
            int keysToDistribute = donor.KeysCount + donee.KeysCount;
            int childKeys = keysToDistribute / 2;
            int neighborKeys = keysToDistribute - childKeys;

            int comparisonIndex = leftToRight ? donor.KeysCount - 1 : 0;
            bool possibleCircularDependency = (neighborKeys == _size - 1) && leftToRight == key.CompareTo(donor.GetKey(comparisonIndex)) > 0;

            if (!leftToRight)
            {
                index--;
            }

            if (!IsFull(donee) && !possibleCircularDependency)
            {
                Distribute(donor, donee, parent, index, leftToRight);
                IndexFixup(key, parent, ref index);
                Insert(key, value, parent.GetChild(index));
                return true;
            }

            return false;
        }

        private void Insert(TKey key, TValue value, IBStarTreeNode pointer)
        {
            int index = FindIndex(pointer, key);

            if (pointer.IsLeaf())
            {
                pointer.InsertValue(index, key, value);
                return;
            }
            IBStarTreeNode child = pointer.GetChild(index);

            if (!IsFull(child))
            {
                Insert(key, value, child);
                return;
            }

            IBStarTreeNode neighbor = null;
            if (pointer.KeysCount > index)
            {
                neighbor = pointer.GetChild(index + 1);

                if (TryDistributeAndInsert(child, neighbor, pointer, key, value, ref index, true))
                {
                    return;
                }
            }

            bool leftNeighbor = false;
            if (index != 0)
            {
                neighbor = pointer.GetChild(index - 1);
                leftNeighbor = true;

                if (TryDistributeAndInsert(child, neighbor, pointer, key, value, ref index, false))
                {
                    return;
                }
            }

            if (leftNeighbor)
            {
                TwoThreeSplit(pointer, neighbor, child, index);
            }
            else
            {
                TwoThreeSplit(pointer, child, neighbor, index);
            }

            IndexFixup(key, pointer, ref index);
            Insert(key, value, pointer.GetChild(index));
            return;
        }

        private void RootFix(IBStarTreeNode root)
        {
            if (!IsFull(root))
            {
                return;
            }

            IBStarTreeNode node = new Node(RootSize);

            var temp = root;
            UpdateRoot(node);
            node.SetChild(0, temp);

            Split(node, temp, 0);
            temp.Shrink(_size);
        }

        public override void Insert(TKey key, TValue value)
        {
            using BTreeContext<TKey, TValue, IBStarTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Insert);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Insert].Count++;
            }

            RootFix(context.Root);
            Insert(key, value, context.Root);
        }

        private void Borrow(IBStarTreeNode pointer, IBStarTreeNode donor, int index, bool leftToRight)
        {
            IBStarTreeNode leaf = donor;
            while (!leaf.IsLeaf())
            {
                leaf = leaf.GetChild(leftToRight ? 0 : leaf.KeysCount);
            }

            var childKey = leaf.GetKey(leftToRight ? 0 : leaf.KeysCount - 1);
            pointer.SetKey(index, childKey);

            var childValue = leaf.GetValue(leftToRight ? 0 : leaf.KeysCount - 1);
            pointer.SetValue(index, childValue);

            Delete(childKey, donor);
        }

        private void PredecessorBorrow(IBStarTreeNode pointer, IBStarTreeNode donor, int index) => Borrow(pointer, donor, index, false);

        private void Merge(IBStarTreeNode donor, IBStarTreeNode donee, IBStarTreeNode parent, int mutualKey, bool leftToRight)
        {
            int keysCount = donor.KeysCount;
            for (int fusionIndex = 0; fusionIndex < keysCount; fusionIndex++)
            {
                LeftTransfer(donor, donee, parent, mutualKey);
            }

            donee.InsertValue(donee.KeysCount, parent.GetKey(mutualKey), parent.GetValue(mutualKey));
            if (!donor.IsLeaf())
            {
                donee.InsertChild(donee.KeysCount, donor.GetChild(0));
                donor.RemoveChild(0);
            }
            parent.RemoveFull(mutualKey, leftToRight);

            if (parent.KeysCount == 0)
            {
                UpdateRoot(donee);
            }
        }

        private void ThreeTwoMerge(IBStarTreeNode leftNeighbor, IBStarTreeNode child, IBStarTreeNode rightNeighbor, IBStarTreeNode parent, int childKey)
        {
            int leftMutualKey = childKey - 1;
            int rightMutualKey = childKey;

            int totalNodes = leftNeighbor.KeysCount + rightNeighbor.KeysCount + child.KeysCount + 1;
            int left = totalNodes / 2;
            int right = totalNodes - left;

            while (leftNeighbor.KeysCount != left && child.KeysCount > 0)
            {
                LeftTransfer(child, leftNeighbor, parent, leftMutualKey);
            }

            while (rightNeighbor.KeysCount > 0)
            {
                LeftTransfer(rightNeighbor, child, parent, rightMutualKey);
            }

            while (leftNeighbor.KeysCount != left)
            {
                LeftTransfer(child, leftNeighbor, parent, leftMutualKey);
            }

            child.InsertValue(child.KeysCount, parent.GetKey(rightMutualKey), parent.GetValue(rightMutualKey));
            if (!rightNeighbor.IsLeaf())
            {
                child.InsertChild(child.KeysCount, rightNeighbor.GetChild(0));
                rightNeighbor.RemoveChild(0);
            }
            parent.RemoveFull(rightMutualKey, true);
        }

        private (IBStarTreeNode child, IBStarTreeNode firstNeighbor, IBStarTreeNode secondNeighbor) GetNeighborsTriple(IBStarTreeNode parent, int index)
        {
            (IBStarTreeNode child, IBStarTreeNode firstNeighbor, IBStarTreeNode secondNeighbor) = index switch
            {
                0 => (parent.GetChild(index), parent.GetChild(index + 1), parent.GetChild(index + 2)),
                int x when x == parent.KeysCount => (parent.GetChild(index), parent.GetChild(index - 1), parent.GetChild(index - 2)),
                _ => (parent.GetChild(index), parent.GetChild(index - 1), parent.GetChild(index + 1))
            };

            return (child, firstNeighbor, secondNeighbor);
        }

        private void Delete(TKey key, IBStarTreeNode pointer)
        {
            if (pointer == null)
            {
                return;
            }

            int index = FindIndex(pointer, key);

            void TryFill(IBStarTreeNode childNode)
            {
                if (pointer.KeysCount == 1)
                {
                    IBStarTreeNode neighbor = index == 0 ? pointer.GetChild(index + 1) : pointer.GetChild(index - 1);

                    if (IsBig(neighbor))
                    {
                        Distribute(neighbor, childNode, pointer, index == 0 ? index : index - 1, index != 0);
                    }

                    return;
                }

                var (_, firstNeighbor, secondNeighbor) = GetNeighborsTriple(pointer, index);

                if (index == 0)
                {
                    if (!IsBig(firstNeighbor) && IsBig(secondNeighbor))
                    {
                        Distribute(secondNeighbor, firstNeighbor, pointer, index + 1, false);
                    }

                    if (IsBig(firstNeighbor))
                    {
                        Distribute(firstNeighbor, childNode, pointer, index, false);
                    }
                }
                else if (index == pointer.KeysCount)
                {
                    if (!IsBig(firstNeighbor) && IsBig(secondNeighbor))
                    {
                        Distribute(secondNeighbor, firstNeighbor, pointer, index - 2, true);
                    }

                    if (IsBig(firstNeighbor))
                    {
                        Distribute(firstNeighbor, childNode, pointer, index - 1, true);
                    }
                }
                else
                {
                    if (IsBig(firstNeighbor))
                    {
                        Distribute(firstNeighbor, childNode, pointer, index - 1, true);
                    }

                    if (!IsBig(childNode))
                    {
                        Distribute(secondNeighbor, childNode, pointer, index, false);
                    }
                }
            }

            void ThreeTwoMergeClosure()
            {
                var (child, firstNeighbor, secondNeighbor) = GetNeighborsTriple(pointer, index);

                if (index == 0)
                {
                    ThreeTwoMerge(child, firstNeighbor, secondNeighbor, pointer, index + 1);
                }
                else if (index == pointer.KeysCount)
                {
                    ThreeTwoMerge(secondNeighbor, firstNeighbor, child, pointer, index - 1);
                }
                else
                {
                    ThreeTwoMerge(firstNeighbor, child, secondNeighbor, pointer, index);
                }
            }

            bool containsKey = index < pointer.KeysCount && key.CompareTo(pointer.GetKey(index)) == 0;
            if (pointer.IsLeaf())
            {
                if (containsKey)
                {
                    pointer.RemoveValue(index);
                }

                return;
            }

            var childNode = pointer.GetChild(index);

            if (containsKey)
            {
                if (!IsBig(childNode))
                {
                    TryFill(childNode);

                    if (IsBig(childNode))
                    {
                        Delete(key, pointer);
                        return;
                    }
                }
                else
                {
                    PredecessorBorrow(pointer, childNode, index);
                    return;
                }

                if (IsRoot(pointer) && pointer.KeysCount == 1)
                {
                    childNode.Expand(RootSize);
                    var childNodeSuccessor = pointer.GetChild(index + 1);
                    Merge(childNodeSuccessor, childNode, pointer, index, true);
                    Delete(key, childNode);
                }
                else
                {
                    ThreeTwoMergeClosure();
                    Delete(key, pointer);
                }

                return;
            }

            if (!IsBig(childNode))
            {
                TryFill(childNode);
            }

            if (IsBig(childNode))
            {
                Delete(key, childNode);
                return;
            }

            int parentKeyIndex = Math.Max(index - 1, 0);

            if (IsRoot(pointer) && pointer.KeysCount == 1)
            {
                var childNodeNeighbor = index == 0 ? pointer.GetChild(index + 1) : pointer.GetChild(index - 1);

                if (index != 0)
                {
                    var temp = childNodeNeighbor;
                    childNodeNeighbor = childNode;
                    childNode = temp;
                }

                childNode.Expand(RootSize);
                Merge(childNodeNeighbor, childNode, pointer, parentKeyIndex, index != 0);
                Delete(key, childNode);
            }
            else
            {
                ThreeTwoMergeClosure();
                Delete(key, pointer);
            }
        }

        public override void Delete(TKey key)
        {
            using BTreeContext<TKey, TValue, IBStarTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Delete);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Delete].Count++;
            }

            Delete(key, context.Root);
        }

        public override Option<TValue> Search(TKey key)
        {
            using BTreeContext<TKey, TValue, IBStarTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Search);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Search].Count++;
            }

            IBStarTreeNode pointer = context.Root;
            while (true)
            {
                int index = FindIndex(pointer, key);

                if (index < pointer.KeysCount && key.CompareTo(pointer.GetKey(index)) == 0)
                {
                    return Option.Some(pointer.GetValue(index));
                }

                if (pointer.IsLeaf())
                {
                    return Option.None<TValue>();
                }

                pointer = pointer.GetChild(index);
            }
        }

        private IEnumerable<(TKey, TValue)> Traverse(Node pointer)
        {
            if (pointer.IsLeaf())
            {
                return pointer.Keys.Zip(pointer.Values).Take(pointer.KeysCount);
            }

            IEnumerable<(TKey, TValue)> accumulator = new List<(TKey, TValue)>();

            for (int i = 0; i < pointer.KeysCount; ++i)
            {
                accumulator = accumulator.Concat(Traverse(pointer.Children[i] as Node));
                accumulator = accumulator.Append((pointer.Keys[i], pointer.Values[i]));
            }
            accumulator = accumulator.Concat(Traverse(pointer.Children[pointer.KeysCount] as Node));

            return accumulator;
        }

        public override IEnumerable<(TKey, TValue)> Traverse()
        {
            return Traverse(_root);
        }

        public void PrettyPrint()
        {
            _root.Print();
            Console.WriteLine();
        }
    }
}
