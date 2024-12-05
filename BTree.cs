using System;
using System.Collections.Generic;
using System.Linq;
using Optional;

namespace HeapsAndBTrees
{
    internal class BTree<TKey, TValue> : 
        DiagnostableBTreeBase<
            TKey, TValue, 
            BTree<TKey, TValue>.IBTreeNode,
            BTree<TKey, TValue>.VirtualNode,
            BTree<TKey, TValue>.Node
        > 
        where TKey : IComparable
    {
        public interface IBTreeNode : INode<TKey>, IValueNode<TKey, TValue>, IInternalNode<IBTreeNode, TKey>
        {
            public void InsertFull(int index, TKey key, IBTreeNode child, TValue value, bool leftToRight);
            public void RemoveFull(int index, bool leftToRight);

            public bool IsLeaf();
            public void Print(int offset = 0);
        }

        public class Node : IBTreeNode, IActualNode<IBTreeNode, TKey>
        {
            private IBTreeNode SelfTyped => this;

            protected int _keysCount;
            public int KeysCount { get { return _keysCount; } set { _keysCount = value; } }

            protected TKey[] _keys;
            public ref TKey[] Keys { get { return ref _keys; } }

            protected TValue[] _values;
            public ref TValue[] Values { get { return ref _values; } }

            protected IBTreeNode[] _children;
            public ref IBTreeNode[] Children { get { return ref _children; } }

            public IBTreeNode GetChild(int index)
            {
                if (Children is null)
                {
                    InitializeChildren();
                }

                return Children[index];
            }

            public void SetChild(int index, IBTreeNode child)
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

            public void InsertChild(int index, IBTreeNode child)
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

            public void InsertFull(int index, TKey key, IBTreeNode child, TValue value, bool leftToRight)
            {
                SelfTyped.InsertValue(index, key, value);
                InsertChild(leftToRight ? index: index + 1, child);
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

            public bool IsLeaf() => Children is null;

            public Node(int size)
            {
                KeysCount = 0;
                Keys = new TKey[size - 1];
                Values = new TValue[size - 1];
            }

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

        internal class VirtualNode : IBTreeNode, IVirtualNode<Node, IBTreeNode, TKey>
        {
            public Node Node { get; protected set; }
            public IBTreeNode NodeTyped => Node;

            private BTreeContext<TKey, TValue, IBTreeNode, VirtualNode, Node> _context;

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

            public ref IBTreeNode[] Children
            {
                get
                {
                    _context.Actualize(this);
                    return ref Node.Children;
                }
            }

            public TKey GetKey(int index)
            {
                _context.Actualize(this);
                return NodeTyped.GetKey(index);
            }

            public void SetKey(int index, TKey key)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.SetKey(index, key);
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

            public IBTreeNode GetChild(int index)
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

            public void SetChild(int index, IBTreeNode child)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.SetChild(index, child);
            }

            public bool IsDirty { get; protected set; } = false;

            public VirtualNode(Node node, BTreeContext<TKey, TValue, IBTreeNode, VirtualNode, Node> context)
            {
                Node = node;
                _context = context;
            }

            public void InsertValue(int index, TKey key, TValue value)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertValue(index, key, value);
            }

            public void InsertChild(int index, IBTreeNode child)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertChild(index, child);
            }

            public void InsertFull(int index, TKey key, IBTreeNode child, TValue value, bool LeftToRight)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.InsertFull(index, key, child, value, LeftToRight);
            }

            public void RemoveValue(int index)
            {
                _context.Actualize(this);
                IsDirty = true;

                NodeTyped.RemoveValue(index);
            }

            public void RemoveChild(int index)
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
        private Node _root;
        
        public BTree(int size)
        {
            _size = size;
            UpdateRoot(new Node(_size));
        }

        private BTreeContext<TKey, TValue, IBTreeNode, VirtualNode, Node> CreateContext(BTreeOperation caller) => 
            new BTreeContext<TKey, TValue, IBTreeNode, VirtualNode, Node>(
                this,
                (n, op) => DiskRead(n, op),
                (n, op) => DiskWrite(n, op),
                (n, context) => new VirtualNode(n, context),
                _root, caller, 3
            );

        public void UpdateRoot(IBTreeNode newRoot)
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
            UpdateRoot(new Node(_size));
        }

        public void DiskWrite(IBTreeNode node, BTreeOperation caller)
        {
            if (!_watched)
            {
                return;
            }

            _diagnosticsData[caller].Writes++;
        }

        public void DiskRead(IBTreeNode node, BTreeOperation caller)
        {
            if (!_watched)
            {
                return;
            }

            _diagnosticsData[caller].Reads++;
        }

        private bool IsFull(IBTreeNode node) => node.KeysCount == (_size - 1);
        private bool IsBig(IBTreeNode node) => node.KeysCount > ((_size / 2) - 1);
        private bool IsRoot(IBTreeNode node) => node switch
        {
            Node n => n == _root,
            VirtualNode v => v.Node == _root,
            _ => throw new ArgumentException("Could not retrieve an actual node")
        };

        private void Split(IBTreeNode parent, IBTreeNode child, int childIndex)
        {
            var node = new Node(_size);
            int median = (_size / 2) - 1;

            if (!child.IsLeaf())
            {
                node.InsertChild(0, child.GetChild(_size-1));
                child.RemoveChild(_size-1);
            }

            parent.InsertFull(childIndex, child.GetKey(_size - 2), node, child.GetValue(_size - 2), false);
            child.RemoveValue(_size - 2);

            for (int index = _size-3; index >= median; index--)
            {
                RightTransfer(child, node, parent, childIndex);
            }
        }

        private int FindIndex(IBTreeNode pointer, TKey key)
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

        private void IndexFixup(TKey key, IBTreeNode pointer, ref int index)
        {
            while (index < pointer.KeysCount && key.CompareTo(pointer.GetKey(index)) > 0)
            {
                index++;
            }
        }

        private void Insert(TKey key, TValue value, IBTreeNode pointer)
        {
            int index = FindIndex(pointer, key);

            if (pointer.IsLeaf())
            {
                pointer.InsertValue(index, key, value);
                return;
            }
            IBTreeNode child = pointer.GetChild(index);

            if (IsFull(child))
            {
                Split(pointer, child, index);
                IndexFixup(key, pointer, ref index);

                Insert(key, value, pointer.GetChild(index));
                return;
            }

            Insert(key, value, child);
        }

        private void RootFix(IBTreeNode root)
        {
            if (!IsFull(root))
            {
                return;
            }

            var node = new Node(_size);

            var temp = root;
            UpdateRoot(node);
            node.SetChild(0, temp);

            Split(node, temp, 0);
        }

        public override void Insert(TKey key, TValue value)
        {
            using BTreeContext<TKey, TValue, IBTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Insert);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Insert].Count++;
            }

            RootFix(context.Root);
            Insert(key, value, context.Root);
        }

        private void Borrow(IBTreeNode pointer, IBTreeNode donor, int index, bool leftToRight)
        {
            IBTreeNode leaf = donor;
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

        private void PredecessorBorrow(IBTreeNode pointer, IBTreeNode donor, int index) => Borrow(pointer, donor, index, false);

        private void SuccessorBorrow(IBTreeNode pointer, IBTreeNode donor, int index) => Borrow(pointer, donor, index, true);

        private void Transfer(IBTreeNode donor, IBTreeNode donee, IBTreeNode parent, int mutualKey, bool leftToRight)
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

        private void LeftTransfer(IBTreeNode donor, IBTreeNode donee, IBTreeNode parent, int mutualKey) => Transfer(donor, donee, parent, mutualKey, false);
        private void RightTransfer(IBTreeNode donor, IBTreeNode donee, IBTreeNode parent, int mutualKey) => Transfer(donor, donee, parent, mutualKey, true);

        private void Merge(IBTreeNode donor, IBTreeNode donee, IBTreeNode parent, int mutualKey, bool leftToRight)
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

            if (IsRoot(parent) && parent.KeysCount == 1)
            {
                UpdateRoot(donee);
            }
            else
            {
                parent.RemoveFull(mutualKey, leftToRight);
            }
        }

        private void Delete(TKey key, IBTreeNode pointer)
        {
            if (pointer == null)
            {
                return;
            }

            int index = FindIndex(pointer, key);
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
                if (IsBig(childNode))
                {
                    PredecessorBorrow(pointer, childNode, index);
                    return;
                }

                var childNodeSuccessor = pointer.GetChild(index + 1);
                if (IsBig(childNodeSuccessor))
                {
                    SuccessorBorrow(pointer, childNodeSuccessor, index);
                    return;
                }

                Merge(childNodeSuccessor, childNode, pointer, index, true);
                Delete(key, childNode);
                return;
            }

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

        public override void Delete(TKey key)
        {
            using BTreeContext<TKey, TValue, IBTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Delete);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Delete].Count++;
            }

            Delete(key, context.Root);
        }

        public override Option<TValue> Search(TKey key)
        {
            using BTreeContext<TKey, TValue, IBTreeNode, VirtualNode, Node> context = CreateContext(BTreeOperation.Search);

            if (_watched)
            {
                _diagnosticsData[BTreeOperation.Search].Count++;
            }

            IBTreeNode pointer = context.Root;
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
