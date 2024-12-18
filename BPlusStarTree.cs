using Optional;

namespace HeapsAndBTrees
{
    public class BPlusStarTree<TKey, TValue> :
        DiagnostableBTreeBase<
            TKey, TValue,
            BPlusStarTree<TKey, TValue>.IBPlusStarTreeNode,
            BPlusStarTree<TKey, TValue>.VirtualNode,
            BPlusStarTree<TKey, TValue>.Node
        >
        where TKey : IComparable
    {
        public interface IBPlusStarTreeNode : INode<TKey>
        {
            public void Expand(int newSize);
            public void Shrink(int newSize);

            public void RemoveFull(int index, bool leftToRight);
            public void Print(int offset = 0);
        }

        public interface IBPlusStarTreeInternalNode : IBPlusStarTreeNode, IInternalNode<IBPlusStarTreeNode, TKey>
        {
            public void InsertKey(int index, TKey key);
            public void RemoveKey(int index);
        }

        public interface IBPlusStarTreeValueNode : IBPlusStarTreeNode, IValueNode<TKey, TValue> { }

        public abstract class Node : IBPlusStarTreeNode, IActualNode<IBPlusStarTreeNode, TKey>
        {
            protected int _keysCount;
            public int KeysCount { get { return _keysCount; } set { _keysCount = value; } }

            protected TKey[] _keys;
            public ref TKey[] Keys { get { return ref _keys; } }

            public virtual TKey GetKey(int index) => Keys[index];
            public virtual void SetKey(int index, TKey key) => Keys[index] = key;


            public abstract void Expand(int newSize);
            public abstract void Shrink(int newSize);

            public abstract void RemoveFull(int index, bool leftToRight);
            public abstract void Print(int offset = 0);
        }

        public class InternalNode : Node, IBPlusStarTreeInternalNode
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

            protected IBPlusStarTreeNode[] _children;
            public ref IBPlusStarTreeNode[] Children { get { return ref _children; } }

            public IBPlusStarTreeNode GetChild(int index)
            {
                if (Children is null)
                {
                    InitializeChildren();
                }

                return Children[index];
            }

            public void SetChild(int index, IBPlusStarTreeNode child)
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

            public void InsertChild(int index, IBPlusStarTreeNode child)
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

            public override void Expand(int newSize)
            {
                var newKeys = new TKey[newSize - 1];
                Array.Copy(Keys, newKeys, Keys.Length);
                Keys = newKeys;

                var newChildren = new Node[newSize];
                Array.Copy(Children, newChildren, Children.Length);
                Children = newChildren;
            }

            public override void Shrink(int newSize)
            {
                var newKeys = new TKey[newSize - 1];
                Array.Copy(Keys, newKeys, newKeys.Length);
                Keys = newKeys;

                var newChildren = new Node[newSize];
                Array.Copy(Children, newChildren, newChildren.Length);
                Children = newChildren;
            }

            public override void RemoveFull(int index, bool leftToRight)
            {
                RemoveKey(index);
                (this as IInternalNode<IBPlusStarTreeNode, TKey>).RemoveChild(leftToRight ? index + 1 : index);
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

        public class LeafNode : Node, IBPlusStarTreeValueNode
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

            public override void Expand(int newSize)
            {
                var newKeys = new TKey[newSize];
                Array.Copy(Keys, newKeys, Keys.Length);
                Keys = newKeys;

                var newValues = new TValue[newSize];
                Array.Copy(Values, newValues, Values.Length);
                Values = newValues;
            }

            public override void Shrink(int newSize)
            {
                var newKeys = new TKey[newSize];
                Array.Copy(Keys, newKeys, newKeys.Length);
                Keys = newKeys;

                var newValues = new TValue[newSize];
                Array.Copy(Values, newValues, newValues.Length);
                Values = newValues;
            }
        }

        public abstract class VirtualNode : IBPlusStarTreeNode, IVirtualNode<Node, IBPlusStarTreeNode, TKey>
        {
            public Node Node { get; protected set; }
            public IBPlusStarTreeNode NodeTyped => Node;

            protected BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node> _context;

            public VirtualNode(Node node, BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node> context)
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

            public virtual void Expand(int newSize)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.Expand(newSize);
            }

            public virtual void Shrink(int newSize)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.Shrink(newSize);
            }

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

        public class VirtualInternalNode : VirtualNode, IBPlusStarTreeInternalNode
        {
            public InternalNode NodeTyped => Node as InternalNode;

            public VirtualInternalNode(InternalNode node, BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node> context) : base(node, context) { }

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

            public ref IBPlusStarTreeNode[] Children
            {
                get
                {
                    _context.Actualize(this);
                    return ref NodeTyped.Children;
                }
            }

            public IBPlusStarTreeNode GetChild(int index)
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

            public void SetChild(int index, IBPlusStarTreeNode child)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.SetChild(index, child);
            }

            public void InsertChild(int index, IBPlusStarTreeNode child)
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

        public class VirtualLeafNode : VirtualNode, IBPlusStarTreeValueNode
        {
            public IValueNode<TKey, TValue> NodeTyped => Node as LeafNode;

            public VirtualLeafNode(LeafNode node, BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node> context) : base(node, context)
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
        private int RootNodeSize => (2 * ((2 * _nodeSize - 2) / 3)) + 1;

        private int _leafSize;
        private int RootLeafSize => (2 * ((2 * _leafSize - 2) / 3)) + 1;

        private int _contextSize;

        private Node _root;

        public BPlusStarTree(int nodeSize, int leafSize, int contextSize = 4)
        {
            _nodeSize = nodeSize;
            _leafSize = leafSize;
            _contextSize = contextSize;
            UpdateRoot(new LeafNode(RootLeafSize));
        }

        private BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node> CreateContext(BTreeOperation caller) =>
            new BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node>(
                this,
                (n, op) => DiskRead(n, op),
                (n, op) => DiskWrite(n, op),
                (n, context) => n switch
                {
                    InternalNode internalNode => new VirtualInternalNode(internalNode, context),
                    LeafNode leafNode => new VirtualLeafNode(leafNode, context),
                },
                _root, caller, _contextSize
            );

        public void UpdateRoot(IBPlusStarTreeNode newRoot)
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
            UpdateRoot(new LeafNode(RootLeafSize));
        }

        public void DiskWrite(IBPlusStarTreeNode node, BTreeOperation caller)
        {
            if (!_watched)
            {
                return;
            }

            _diagnosticsData[caller].Writes++;
        }

        public void DiskRead(IBPlusStarTreeNode node, BTreeOperation caller)
        {
            if (!_watched)
            {
                return;
            }

            _diagnosticsData[caller].Reads++;
        }

        private bool IsFull(IBPlusStarTreeNode node) => node switch
        {
            IBPlusStarTreeValueNode leafNode => leafNode.KeysCount == _leafSize,
            IBPlusStarTreeInternalNode internalNode => internalNode.KeysCount == (_nodeSize - 1),
        };

        private bool IsBig(IBPlusStarTreeNode node) => node switch
        {
            IBPlusStarTreeValueNode leafNode => leafNode.KeysCount > ((_leafSize / 2) - 1),
            IBPlusStarTreeInternalNode internalNode => internalNode.KeysCount > ((_nodeSize / 2) - 1),
        };

        private bool IsRoot(IBPlusStarTreeNode node) => node switch
        {
            Node n => n == _root,
            VirtualNode v => v.Node == _root,
            _ => throw new ArgumentException("Could not retrieve an actual node")
        };

        private int FindIndex(IBPlusStarTreeNode pointer, TKey key)
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

        private void IndexFixup(TKey key, IBPlusStarTreeNode pointer, ref int index)
        {
            while (index < pointer.KeysCount && key.CompareTo(pointer.GetKey(index)) > 0)
            {
                index++;
            }
        }

        private void Transfer(IBPlusStarTreeInternalNode donor, IBPlusStarTreeInternalNode donee, IBPlusStarTreeInternalNode parent, int mutualKey, bool leftToRight)
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

        private void Transfer(IBPlusStarTreeValueNode donor, IBPlusStarTreeValueNode donee, IBPlusStarTreeInternalNode parent, int mutualKey, bool leftToRight)
        {
            int insertIndex = leftToRight ? 0 : donee.KeysCount;
            int transferIndex = leftToRight ? donor.KeysCount - 1 : 0;

            TKey transferKey = donor.GetKey(transferIndex);
            donee.InsertValue(insertIndex, transferKey, donor.GetValue(transferIndex));

            donor.RemoveFull(transferIndex, leftToRight);
            parent.SetKey(mutualKey, leftToRight ? donor.GetKey(transferIndex - 1) : transferKey);
        }

        private void Transfer<T>(T donor, T donee, IBPlusStarTreeInternalNode parent, int mutualKey, bool leftToRight) where T : IBPlusStarTreeNode
        {
            switch ((donor, donee))
            {
                case (IBPlusStarTreeValueNode donorLeaf, IBPlusStarTreeValueNode doneeLeaf):
                    Transfer(donorLeaf, doneeLeaf, parent, mutualKey, leftToRight);
                    break;
                case (IBPlusStarTreeInternalNode donorNode, IBPlusStarTreeInternalNode doneeNode):
                    Transfer(donorNode, doneeNode, parent, mutualKey, leftToRight);
                    break;
            }
        }

        private void LeftTransfer<T>(T donor, T donee, IBPlusStarTreeInternalNode parent, int mutualKey) where T : IBPlusStarTreeNode => Transfer(donor, donee, parent, mutualKey, false);
        private void RightTransfer<T>(T donor, T donee, IBPlusStarTreeInternalNode parent, int mutualKey) where T : IBPlusStarTreeNode => Transfer(donor, donee, parent, mutualKey, true);

        private void Split(IBPlusStarTreeInternalNode parent, IBPlusStarTreeInternalNode child, int childIndex)
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

        private void Split(IBPlusStarTreeInternalNode parent, IBPlusStarTreeValueNode child, int childIndex)
        {
            var node = new LeafNode(_leafSize);
            int size = child.KeysCount + 1;
            int median = (size / 2) - 1;

            parent.InsertKey(childIndex, child.GetKey(size - 1));

            parent.InsertChild(childIndex + 1, node);
            for (int index = size - 2; index >= median; index--)
            {
                RightTransfer(child, node, parent, childIndex);
            }
        }

        private void Split(IBPlusStarTreeInternalNode parent, IBPlusStarTreeNode child, int childIndex)
        {
            switch (child)
            {
                case IBPlusStarTreeValueNode leafNode:
                    Split(parent, leafNode, childIndex);
                    break;
                case IBPlusStarTreeInternalNode internalNode:
                    Split(parent, internalNode, childIndex);
                    break;
            }
        }

        private void TwoThreeSplit(IBPlusStarTreeInternalNode parent, IBPlusStarTreeValueNode child, IBPlusStarTreeValueNode sibling, int childIndex)
        {
            var node = new LeafNode(_leafSize);
            var size = _leafSize;
            int median = (size / 3) - 1;

            parent.InsertKey(childIndex, sibling.GetKey(sibling.KeysCount - 1));

            parent.InsertChild(childIndex + 2, node);
            for (int index = sibling.KeysCount - 1; index > median; index--)
            {
                RightTransfer(sibling, node, parent, childIndex + 1);
            }

            median = (2 * size / 3) - 1;
            while (child.KeysCount > median)
            {
                RightTransfer(child, sibling, parent, childIndex);
            }
        }

        private void TwoThreeSplit(IBPlusStarTreeInternalNode parent, IBPlusStarTreeInternalNode child, IBPlusStarTreeInternalNode sibling, int childIndex)
        {
            var node = new InternalNode(_nodeSize);
            var size = _nodeSize;
            int median = (size / 3) - 1;

            node.InsertChild(0, sibling.GetChild(sibling.KeysCount));
            parent.InsertKey(childIndex + 1, sibling.GetKey(sibling.KeysCount - 1));
            sibling.RemoveFull(sibling.KeysCount - 1, true);

            parent.InsertChild(childIndex + 2, node);
            for (int index = sibling.KeysCount - 1; index > median; index--)
            {
                RightTransfer(sibling, node, parent, childIndex + 1);
            }

            median = (2 * size / 3) - 1;
            while (child.KeysCount > median)
            {
                RightTransfer(child, sibling, parent, childIndex);
            }
        }

        private void TwoThreeSplit(IBPlusStarTreeInternalNode parent, IBPlusStarTreeNode child, IBPlusStarTreeNode sibling, int childIndex)
        {
            switch ((child, sibling))
            {
                case (IBPlusStarTreeValueNode leafNodeChild, IBPlusStarTreeValueNode leafNodeSibling):
                    TwoThreeSplit(parent, leafNodeChild, leafNodeSibling, childIndex);
                    break;
                case (IBPlusStarTreeInternalNode internalNodeChild, IBPlusStarTreeInternalNode internalNodeSibling):
                    TwoThreeSplit(parent, internalNodeChild, internalNodeSibling, childIndex);
                    break;
            }
        }

        private void Insert(TKey key, TValue value, IBPlusStarTreeValueNode pointer)
        {
            int index = FindIndex(pointer, key);

            pointer.InsertValue(index, key, value);
            return;
        }

        private void Distribute(IBPlusStarTreeNode donor, IBPlusStarTreeNode donee, IBPlusStarTreeInternalNode parent, int index, bool leftToRight)
        {
            int totalKeys = donor.KeysCount + donee.KeysCount;
            int childKeys = totalKeys - (totalKeys / 2);

            while (donee.KeysCount != childKeys)
            {
                Transfer(donor, donee, parent, index, leftToRight);
            }
        }

        private bool TryDistributeAndInsert(IBPlusStarTreeNode donor, IBPlusStarTreeNode donee, IBPlusStarTreeInternalNode parent, TKey key, TValue value, ref int index, bool leftToRight)
        {
            int keysToDistribute = donor.KeysCount + donee.KeysCount;
            int childKeys = keysToDistribute / 2;
            int neighborKeys = keysToDistribute - childKeys;

            int comparisonIndex = leftToRight ? donor.KeysCount - 2 : 0;
            bool possibleCircularDependency = (neighborKeys == donor.KeysCount) && leftToRight == key.CompareTo(donor.GetKey(comparisonIndex)) > 0;

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

        private void Insert(TKey key, TValue value, IBPlusStarTreeInternalNode pointer)
        {
            int index = FindIndex(pointer, key);

            IBPlusStarTreeNode child = pointer.GetChild(index);

            if (!IsFull(child))
            {
                Insert(key, value, child);
                return;
            }

            IBPlusStarTreeNode neighbor = null;
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

        private void Insert(TKey key, TValue value, IBPlusStarTreeNode pointer)
        {
            switch (pointer)
            {
                case IBPlusStarTreeValueNode leafNode:
                    Insert(key, value, leafNode);
                    break;
                case IBPlusStarTreeInternalNode internalNode:
                    Insert(key, value, internalNode);
                    break;
            }
        }

        private void RootFix(IBPlusStarTreeNode root)
        {
            if (!IsFull(root))
            {
                return;
            }

            var node = new InternalNode(RootNodeSize);

            var temp = root;
            UpdateRoot(node);
            node.SetChild(0, temp);

            Split(node, temp, 0);
            temp.Shrink(temp is IBPlusStarTreeInternalNode ? _nodeSize : _leafSize);
        }

        public override void Insert(TKey key, TValue value)
        {
            using BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Insert);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Insert].Count++;
            }

            RootFix(context.Root);
            Insert(key, value, context.Root);
        }

        private void Merge(IBPlusStarTreeNode donor, IBPlusStarTreeNode donee, IBPlusStarTreeInternalNode parent, int mutualKey, bool leftToRight)
        {
            int keysCount = donor.KeysCount;
            for (int fusionIndex = 0; fusionIndex < keysCount; fusionIndex++)
            {
                LeftTransfer(donor, donee, parent, mutualKey);
            }

            if (donee is IBPlusStarTreeInternalNode doneeTyped && donor is IBPlusStarTreeInternalNode donorTyped)
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

        private void ThreeTwoMerge(IBPlusStarTreeNode leftNeighbor, IBPlusStarTreeNode child, IBPlusStarTreeNode rightNeighbor, IBPlusStarTreeInternalNode parent, int childKey)
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

            if (child is IBPlusStarTreeInternalNode childTyped && rightNeighbor is IBPlusStarTreeInternalNode rightNeighborTyped)
            {
                childTyped.InsertChild(child.KeysCount + 1, rightNeighborTyped.GetChild(0));
                childTyped.InsertKey(child.KeysCount, parent.GetKey(rightMutualKey));
            }
            parent.RemoveFull(rightMutualKey, true);
        }

        private (IBPlusStarTreeNode child, IBPlusStarTreeNode firstNeighbor, IBPlusStarTreeNode secondNeighbor) GetNeighborsTriple(IBPlusStarTreeInternalNode parent, int index)
        {
            (IBPlusStarTreeNode child, IBPlusStarTreeNode firstNeighbor, IBPlusStarTreeNode secondNeighbor) = index switch
            {
                0 => (parent.GetChild(index), parent.GetChild(index + 1), parent.GetChild(index + 2)),
                int x when x == parent.KeysCount => (parent.GetChild(index), parent.GetChild(index - 1), parent.GetChild(index - 2)),
                _ => (parent.GetChild(index), parent.GetChild(index - 1), parent.GetChild(index + 1))
            };

            return (child, firstNeighbor, secondNeighbor);
        }

        private void Delete(TKey key, IBPlusStarTreeInternalNode pointer)
        {
            int index = FindIndex(pointer, key);

            void TryFill(IBPlusStarTreeNode childNode)
            {
                if (pointer.KeysCount == 1)
                {
                    IBPlusStarTreeNode neighbor = index == 0 ? pointer.GetChild(index + 1) : pointer.GetChild(index - 1);
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

            var childNode = pointer.GetChild(index);

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

                childNode.Expand(childNode is IBPlusStarTreeValueNode ? RootLeafSize : RootNodeSize);
                Merge(childNodeNeighbor, childNode, pointer, parentKeyIndex, index != 0);
                Delete(key, childNode);
            }
            else
            {
                ThreeTwoMergeClosure();
                Delete(key, pointer);
            }
        }

        private void Delete(TKey key, IBPlusStarTreeValueNode pointer)
        {
            int index = FindIndex(pointer, key);
            if (index < pointer.KeysCount && key.CompareTo(pointer.GetKey(index)) == 0)
            {
                pointer.RemoveValue(index);
            }

            return;
        }

        private void Delete(TKey key, IBPlusStarTreeNode pointer)
        {
            if (pointer == null)
            {
                return;
            }

            switch (pointer)
            {
                case IBPlusStarTreeValueNode leafNode:
                    Delete(key, leafNode);
                    break;
                case IBPlusStarTreeInternalNode internalNode:
                    Delete(key, internalNode);
                    break;
            }
        }

        public override void Delete(TKey key)
        {
            using BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Delete);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Delete].Count++;
            }

            Delete(key, context.Root);
        }

        public override Option<TValue> Search(TKey key)
        {
            using BTreeContext<TKey, TValue, IBPlusStarTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Search);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Search].Count++;
            }

            IBPlusStarTreeNode pointer = context.Root;
            while (pointer is IBPlusStarTreeInternalNode internalNode)
            {
                int index = FindIndex(internalNode, key);
                pointer = internalNode.GetChild(index);
            }

            int leafIndex = FindIndex(pointer, key);
            if (leafIndex < pointer.KeysCount && key.CompareTo(pointer.GetKey(leafIndex)) == 0)
            {
                return Option.Some((pointer as IBPlusStarTreeValueNode).GetValue(leafIndex));
            }

            return Option.None<TValue>();
        }

        private IEnumerable<(TKey, TValue)> Traverse(IBPlusStarTreeValueNode pointer)
        {
            IEnumerable<(TKey, TValue)> accumulator = new List<(TKey, TValue)>();

            for (int i = 0; i <= pointer.KeysCount; ++i)
            {
                accumulator = accumulator.Append((pointer.GetKey(i), pointer.GetValue(i)));
            }

            return accumulator;
        }

        private IEnumerable<(TKey, TValue)> Traverse(IBPlusStarTreeInternalNode pointer)
        {
            IEnumerable<(TKey, TValue)> accumulator = new List<(TKey, TValue)>();

            for (int i = 0; i <= pointer.KeysCount; ++i)
            {
                accumulator = accumulator.Concat(Traverse(pointer.GetChild(i)));
            }

            return accumulator;
        }

        private IEnumerable<(TKey, TValue)> Traverse(IBPlusStarTreeNode pointer) => pointer switch
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
