using Optional;

namespace HeapsAndBTrees
{
    public class BPlusTree<TKey, TValue> :
        DiagnostableBTreeBase<
            TKey, TValue,
            BPlusTree<TKey, TValue>.IBPlusTreeNode,
            BPlusTree<TKey, TValue>.VirtualNode,
            BPlusTree<TKey, TValue>.Node
        >
        where TKey : IComparable
    {
        public interface IBPlusTreeNode : INode<TKey>
        {
            public void RemoveFull(int index, bool leftToRight);
            public void Print(int offset = 0);
        }

        public interface IBPlusTreeInternalNode : IBPlusTreeNode, IInternalNode<IBPlusTreeNode, TKey>
        {
            public void InsertKey(int index, TKey key);
            public void RemoveKey(int index);
        }

        public interface IBPlusTreeValueNode : IBPlusTreeNode, IValueNode<TKey, TValue> { }

        public abstract class Node : IBPlusTreeNode, IActualNode<IBPlusTreeNode, TKey>
        {
            protected int _keysCount;
            public int KeysCount { get { return _keysCount; } set { _keysCount = value; } }

            protected TKey[] _keys;
            public ref TKey[] Keys { get { return ref _keys; } }

            public virtual TKey GetKey(int index) => Keys[index];
            public virtual void SetKey(int index, TKey key) => Keys[index] = key;

            public abstract void RemoveFull(int index, bool leftToRight);
            public abstract void Print(int offset = 0);
        }

        public class InternalNode : Node, IBPlusTreeInternalNode
        {
            public void InsertKey(int index, TKey key)
            {
                if (index < KeysCount)
                {
                    Utilities.ShiftArrayRight(ref Keys, index, KeysCount);
                }

                Keys[index] = key;
                KeysCount++;
            }

            public void RemoveKey(int index)
            {
                Keys[index] = default;

                if (index != KeysCount - 1)
                {
                    Utilities.ShiftArrayLeft(ref Keys, index, KeysCount - 1);
                }
                KeysCount--;
            }

            protected IBPlusTreeNode[] _children;
            public ref IBPlusTreeNode[] Children { get { return ref _children; } }

            public IBPlusTreeNode GetChild(int index)
            {
                if (Children is null)
                {
                    InitializeChildren();
                }

                return Children[index];
            }

            public void SetChild(int index, IBPlusTreeNode child)
            {
                if (Children is null)
                {
                    InitializeChildren();
                }

                Node childTyped = child switch
                {
                    Node node => node,
                    VirtualNode virtualNode => virtualNode.Node as Node,
                    _ => throw new ArgumentException("Could not retrieve an actual node")
                };

                Children[index] = childTyped;
            }

            protected void InitializeChildren()
            {
                int size = Keys.Length + 1;
                Children = new Node[size];
            }

            public void InsertChild(int index, IBPlusTreeNode child)
            {
                if (Children is null)
                {
                    InitializeChildren();
                }

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

            public InternalNode(int size)
            {
                KeysCount = 0;
                Keys = new TKey[size - 1];
            }

            public override void RemoveFull(int index, bool leftToRight)
            {
                RemoveKey(index);
                (this as IInternalNode<IBPlusTreeNode, TKey>).RemoveChild(leftToRight ? index + 1 : index);
            }

            public override void Print(int offset = 0)
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

        public class LeafNode : Node, IBPlusTreeValueNode
        {
            protected TValue[] _values;
            public ref TValue[] Values { get { return ref _values; } }

            public LeafNode(int size)
            {
                KeysCount = 0;
                Keys = new TKey[size];
                Values = new TValue[size];
            }

            public override void RemoveFull(int index, bool leftToRight)
            {
                (this as IValueNode<TKey, TValue>).RemoveValue(index);
            }

            public override void Print(int offset = 0)
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
                Console.WriteLine();
            }
        }

        public abstract class VirtualNode : IBPlusTreeNode, IVirtualNode<Node, IBPlusTreeNode, TKey>
        {
            public Node Node { get; protected set; }
            public IBPlusTreeNode NodeTyped => Node;

            protected BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node> _context;

            public VirtualNode(Node node, BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node> context)
            {
                Node = node;
                _context = context;
            }

            public bool IsDirty { get; protected set; } = false;

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

            public virtual TKey GetKey(int index) => Keys[index];
            public virtual void SetKey(int index, TKey key) => Keys[index] = key;

            public virtual void RemoveFull(int index, bool leftToRight)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.RemoveFull(index, leftToRight);
            }

            public virtual void Print(int offset = 0)
            {
                _context.Actualize(this);
                NodeTyped.Print(offset);
            }
        }

        public class VirtualInternalNode : VirtualNode, IBPlusTreeInternalNode
        {
            public InternalNode NodeTyped => Node as InternalNode;

            public VirtualInternalNode(InternalNode node, BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node> context) : base(node, context) { }

            public void InsertKey(int index, TKey key)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertKey(index, key);
            }

            public void RemoveKey(int index)
            {
                _context.Actualize(this);
                IsDirty = true;
            }

            public ref IBPlusTreeNode[] Children
            {
                get
                {
                    _context.Actualize(this);
                    return ref NodeTyped.Children;
                }
            }

            public IBPlusTreeNode GetChild(int index)
            {
                _context.Actualize(this);
                Node child = NodeTyped.GetChild(index) as Node;

                return child switch
                {
                    InternalNode internalNode => new VirtualInternalNode(internalNode, _context),
                    LeafNode leafNode => new VirtualLeafNode(leafNode, _context),
                    null => throw new ArgumentException("Requested child was null"),
                    _ => throw new ArgumentException("Child has unknown type")
                };
            }

            public void SetChild(int index, IBPlusTreeNode child)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.SetChild(index, child);
            }

            public void InsertChild(int index, IBPlusTreeNode child)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertChild(index, child);
            }

            public void RemoveChild(int index)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.RemoveChild(index);
            }
        }

        public class VirtualLeafNode : VirtualNode, IBPlusTreeValueNode
        {
            public IValueNode<TKey, TValue> NodeTyped => Node as LeafNode;

            public VirtualLeafNode(LeafNode node, BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node> context) : base(node, context)
            { }

            public ref TValue[] Values
            {
                get
                {
                    _context.Actualize(this);
                    return ref (NodeTyped as LeafNode).Values;
                }
            }

            public TValue GetValue(int index)
            {
                _context.Actualize(this);
                return NodeTyped.GetValue(index);
            }

            public void SetValue(int index, TValue key)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.SetValue(index, key);
            }

            public void InsertValue(int index, TKey key, TValue value)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertValue(index, key, value);
            }

            public void RemoveValue(int index)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.RemoveValue(index);
            }
        }

        private int _nodeSize;
        private int _leafSize;
        private Node _root;

        public BPlusTree(int nodeSize, int leafSize)
        {
            _nodeSize = nodeSize;
            _leafSize = leafSize;
            UpdateRoot(new LeafNode(_leafSize));
        }

        private BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node> CreateContext(BTreeOperation caller) =>
            new BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node>(
                this,
                (n, op) => DiskRead(n, op),
                (n, op) => DiskWrite(n, op),
                (n, context) => n switch
                {
                    InternalNode internalNode => new VirtualInternalNode(internalNode, context),
                    LeafNode leafNode => new VirtualLeafNode(leafNode, context),
                },
                _root, caller, 3
            );

        public void UpdateRoot(IBPlusTreeNode newRoot)
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
            UpdateRoot(new LeafNode(_leafSize));
        }

        public void DiskWrite(IBPlusTreeNode node, BTreeOperation caller)
        {
            if (!_watched)
            {
                return;
            }

            _diagnosticsData[caller].Writes++;
        }

        public void DiskRead(IBPlusTreeNode node, BTreeOperation caller)
        {
            if (!_watched)
            {
                return;
            }

            _diagnosticsData[caller].Reads++;
        }

        private bool IsFull(IBPlusTreeNode node) => node switch
        {
            IBPlusTreeValueNode leafNode => leafNode.KeysCount == _leafSize,
            IBPlusTreeInternalNode internalNode => internalNode.KeysCount == (_nodeSize - 1),
        };

        private bool IsBig(IBPlusTreeNode node) => node switch
        {
            IBPlusTreeValueNode leafNode => leafNode.KeysCount > ((_leafSize / 2) - 1),
            IBPlusTreeInternalNode internalNode => internalNode.KeysCount > ((_nodeSize / 2) - 1),
        };

        private bool IsRoot(IBPlusTreeNode node) => node switch
        {
            Node n => n == _root,
            VirtualNode v => v.Node == _root,
            _ => throw new ArgumentException("Could not retrieve an actual node")
        };

        private int FindIndex(IBPlusTreeNode pointer, TKey key)
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

        private void IndexFixup(TKey key, IBPlusTreeNode pointer, ref int index)
        {
            while (index < pointer.KeysCount && key.CompareTo(pointer.GetKey(index)) > 0)
            {
                index++;
            }
        }

        private void Transfer(IBPlusTreeInternalNode donor, IBPlusTreeInternalNode donee, IBPlusTreeInternalNode parent, int mutualKey, bool leftToRight)
        {
            int insertIndex = leftToRight ? 0 : donee.KeysCount;
            int transferIndex = leftToRight ? donor.KeysCount - 1 : 0;
            int childTransferIndex = leftToRight ? transferIndex + 1 : transferIndex;

            TKey transferKey = donor.GetKey(transferIndex);
            donee.InsertKey(insertIndex, parent.GetKey(mutualKey));
            donee.InsertChild(leftToRight ? insertIndex : insertIndex + 1, donor.GetChild(childTransferIndex));

            donor.RemoveFull(transferIndex, leftToRight);
            parent.SetKey(mutualKey, transferKey);
        }

        private void Transfer(IBPlusTreeValueNode donor, IBPlusTreeValueNode donee, IBPlusTreeInternalNode parent, int mutualKey, bool leftToRight)
        {
            int insertIndex = leftToRight ? 0 : donee.KeysCount;
            int transferIndex = leftToRight ? donor.KeysCount - 1 : 0;

            TKey transferKey = donor.GetKey(transferIndex);
            donee.InsertValue(insertIndex, transferKey, donor.GetValue(transferIndex));

            donor.RemoveFull(transferIndex, leftToRight);
            parent.SetKey(mutualKey, leftToRight ? donor.GetKey(transferIndex - 1) : transferKey);
        }

        private void Transfer<T>(T donor, T donee, IBPlusTreeInternalNode parent, int mutualKey, bool leftToRight) where T : IBPlusTreeNode
        {
            switch ((donor, donee))
            {
                case (IBPlusTreeValueNode donorLeaf, IBPlusTreeValueNode doneeLeaf):
                    Transfer(donorLeaf, doneeLeaf, parent, mutualKey, leftToRight);
                    break;
                case (IBPlusTreeInternalNode donorNode, IBPlusTreeInternalNode doneeNode):
                    Transfer(donorNode, doneeNode, parent, mutualKey, leftToRight);
                    break;
            }
        }

        private void LeftTransfer<T>(T donor, T donee, IBPlusTreeInternalNode parent, int mutualKey) where T : IBPlusTreeNode => Transfer(donor, donee, parent, mutualKey, false);
        private void RightTransfer<T>(T donor, T donee, IBPlusTreeInternalNode parent, int mutualKey) where T : IBPlusTreeNode => Transfer(donor, donee, parent, mutualKey, true);

        private void Split(IBPlusTreeInternalNode parent, IBPlusTreeInternalNode child, int childIndex)
        {
            var node = new InternalNode(_nodeSize);
            int size = child.KeysCount + 1;
            int median = (size / 2) - 1;

            node.InsertChild(0, child.GetChild(size - 1));
            parent.InsertKey(childIndex, child.GetKey(size - 2));
            child.RemoveFull(size - 2, true);

            parent.InsertChild(childIndex + 1, node);
            for (int index = size - 3; index >= median; index--)
            {
                RightTransfer(child, node, parent, childIndex);
            }
        }

        private void Split(IBPlusTreeInternalNode parent, IBPlusTreeValueNode child, int childIndex)
        {
            var node = new LeafNode(_leafSize);
            int size = child.KeysCount;
            int median = (size / 2) - 1;

            parent.InsertKey(childIndex, child.GetKey(size - 1));

            parent.InsertChild(childIndex + 1, node);
            for (int index = size - 2; index >= median; index--)
            {
                RightTransfer(child, node, parent, childIndex);
            }
        }

        private void Split(IBPlusTreeInternalNode parent, IBPlusTreeNode child, int childIndex)
        {
            switch (child)
            {
                case IBPlusTreeValueNode leafNode:
                    Split(parent, leafNode, childIndex);
                    break;
                case IBPlusTreeInternalNode internalNode:
                    Split(parent, internalNode, childIndex);
                    break;
            }
        }

        private void Insert(TKey key, TValue value, IBPlusTreeValueNode pointer)
        {
            int index = FindIndex(pointer, key);

            pointer.InsertValue(index, key, value);
            return;
        }

        private void Insert(TKey key, TValue value, IBPlusTreeInternalNode pointer)
        {
            int index = FindIndex(pointer, key);

            IBPlusTreeNode child = pointer.GetChild(index);
            if (IsFull(child))
            {
                Split(pointer, child, index);
                IndexFixup(key, pointer, ref index);

                Insert(key, value, pointer.GetChild(index));
                return;
            }

            Insert(key, value, child);
        }

        private void Insert(TKey key, TValue value, IBPlusTreeNode pointer)
        {
            switch (pointer)
            {
                case IBPlusTreeValueNode leafNode:
                    Insert(key, value, leafNode);
                    break;
                case IBPlusTreeInternalNode internalNode:
                    Insert(key, value, internalNode);
                    break;
            }
        }

        private void RootFix(IBPlusTreeNode root)
        {
            if (!IsFull(root))
            {
                return;
            }

            var node = new InternalNode(_nodeSize);

            var temp = root;
            UpdateRoot(node);
            node.SetChild(0, temp);

            Split(node, temp, 0);
        }

        public override void Insert(TKey key, TValue value)
        {
            using BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Insert);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Insert].Count++;
            }

            RootFix(context.Root);
            Insert(key, value, context.Root);
        }

        private void Merge(IBPlusTreeNode donor, IBPlusTreeNode donee, IBPlusTreeInternalNode parent, int mutualKey, bool leftToRight)
        {
            int keysCount = donor.KeysCount;
            for (int fusionIndex = 0; fusionIndex < keysCount; fusionIndex++)
            {
                LeftTransfer(donor, donee, parent, mutualKey);
            }

            if (donee is IBPlusTreeInternalNode doneeTyped && donor is IBPlusTreeInternalNode donorTyped)
            {
                doneeTyped.InsertChild(donee.KeysCount + 1, donorTyped.GetChild(0));
                doneeTyped.InsertKey(donee.KeysCount, parent.GetKey(mutualKey));
            }
            parent.RemoveFull(mutualKey, leftToRight);

            if (IsRoot(parent) && parent.KeysCount == 0)
            {
                UpdateRoot(donee);
            }
        }

        private void Delete(TKey key, IBPlusTreeInternalNode pointer)
        {
            if (pointer == null)
            {
                return;
            }

            int index = FindIndex(pointer, key);

            var childNode = pointer.GetChild(index);
            if (IsBig(childNode))
            {
                Delete(key, childNode);
                return;
            }

            var childNodeNeighbor = index == 0 ? pointer.GetChild(index + 1) : pointer.GetChild(index - 1);
            int parentKeyIndex = Math.Max(0, index - 1);

            if (IsBig(childNodeNeighbor))
            {
                if (index == 0)
                {
                    LeftTransfer(childNodeNeighbor, childNode, pointer, index);
                }
                else
                {
                    RightTransfer(childNodeNeighbor, childNode, pointer, index - 1);
                }

                Delete(key, childNode);
                return;
            }

            if (index != 0)
            {
                var temp = childNodeNeighbor;
                childNodeNeighbor = childNode;
                childNode = temp;
            }
            else
            {
                index++;
            }

            Merge(childNodeNeighbor, childNode, pointer, parentKeyIndex, index != 0);
            Delete(key, childNode);
            return;
        }

        private void Delete(TKey key, IBPlusTreeValueNode pointer)
        {
            int index = FindIndex(pointer, key);
            if (index < pointer.KeysCount && key.CompareTo(pointer.GetKey(index)) == 0)
            {
                pointer.RemoveValue(index);
            }

            return;
        }

        private void Delete(TKey key, IBPlusTreeNode pointer)
        {
            switch (pointer)
            {
                case IBPlusTreeValueNode leafNode:
                    Delete(key, leafNode);
                    break;
                case IBPlusTreeInternalNode internalNode:
                    Delete(key, internalNode);
                    break;
            }
        }

        public override void Delete(TKey key)
        {
            using BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Delete);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Delete].Count++;
            }

            Delete(key, context.Root);
        }

        public override Option<TValue> Search(TKey key)
        {
            using BTreeContext<TKey, TValue, IBPlusTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Search);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Search].Count++;
            }

            IBPlusTreeNode pointer = context.Root;
            while (pointer is IBPlusTreeInternalNode internalNode)
            {
                int index = FindIndex(internalNode, key);
                pointer = internalNode.GetChild(index);
            }

            int leafIndex = FindIndex(pointer, key);
            if (leafIndex < pointer.KeysCount && key.CompareTo(pointer.GetKey(leafIndex)) == 0)
            {
                return Option.Some((pointer as IBPlusTreeValueNode).GetValue(leafIndex));
            }

            return Option.None<TValue>();
        }

        private IEnumerable<(TKey, TValue)> Traverse(LeafNode pointer)
        {
            return pointer.Keys.Zip(pointer.Values).Take(pointer.KeysCount);
        }

        private IEnumerable<(TKey, TValue)> Traverse(InternalNode pointer)
        {
            IEnumerable<(TKey, TValue)> accumulator = new List<(TKey, TValue)>();

            for (int i = 0; i <= pointer.KeysCount; ++i)
            {
                accumulator = accumulator.Concat(Traverse(pointer.Children[i] as Node));
            }

            return accumulator;
        }

        private IEnumerable<(TKey, TValue)> Traverse(Node pointer) => pointer switch
        {
            LeafNode leafNode => Traverse(leafNode),
            InternalNode internalNode => Traverse(internalNode),
        };

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
