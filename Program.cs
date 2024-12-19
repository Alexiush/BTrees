using HeapsAndBTrees;

void UsageExample<TNode, TVirtual, TActual>(IDiagnostableBTree<char, int, TNode, TVirtual, TActual> bTree)
    where TNode : INode<char>
    where TActual : IActualNode<TNode, char>, TNode
    where TVirtual : IVirtualNode<TActual, TNode, char>, TNode
{
    List<(char Key, int Value)> entries = new List<(char, int)>
    {
        ('F', 0),
        ('S', 1),
        ('Q', 2),
        ('K', 3),
        ('C', 4),
        ('L', 5),
        ('H', 6),
        ('T', 7),
        ('V', 8),
        ('W', 9),
        ('M', 10),
        ('R', 11),
        ('N', 12),
        ('P', 13),
        ('A', 14),
        ('B', 15),
        ('X', 16),
        ('Y', 17),
        ('D', 18),
        ('Z', 19),
        ('E', 20),
    };

    entries.ForEach(e =>
    {
        bTree.Insert(e.Key, e.Value);
        bTree.PrettyPrint();
    });
    entries.ForEach(e => bTree.Search(e.Key).Match(
        n => { Console.WriteLine($"There is value {n} that corresponds to key {e.Key}"); },
        () => { Console.WriteLine($"There is no key {e.Key} in the tree"); }
    ));

    bTree.PrettyPrint();
    Console.WriteLine();

    List<char> removeQueries = new List<char>
{
    'B', 'S', 'A', 'C', 'Y', 'R', 'N', 'H', 'K', 'L', 'V', 'Z', 'X', 'T', 'M', 'E', 'P', 'Q', 'W', 'D',
};
    removeQueries.ForEach(e =>
    {
        bTree.Delete(e);
        bTree.PrettyPrint();
    });
}

void TestBST<TNode, TVirtual, TActual>(IDiagnostableBTree<uint, uint, TNode, TVirtual, TActual> bTree, bool debug = false)
    where TNode : INode<uint>
    where TActual : IActualNode<TNode, uint>, TNode
    where TVirtual : IVirtualNode<TActual, TNode, uint>, TNode
{
    var random = new Random(42);

    var insertedElementsQueue = new Queue<uint>();
    var insertedElementsSet = new HashSet<uint>();

    uint GetNewKey()
    {
        uint key = (uint)random.Next();
        while (insertedElementsSet.Contains(key))
        {
            key = (uint)random.Next();
        }

        return key;
    }

    void IntegrityCheck()
    {
        var expected = insertedElementsSet.Order();
        var actual = bTree.Traverse().Select(kv => kv.Item1);

        if (!expected.SequenceEqual(actual))
        {
            bTree.PrettyPrint();
            throw new Exception("Integrity lost");
        }
    }

    for (int test = 0; test < 10; test++)
    {
        Console.WriteLine($"Test {test + 1}");
        for (int i = 0; i < 1000000; i++)
        {
            uint insertKey = GetNewKey();
            bTree.Insert(insertKey, (uint)random.Next());
            insertedElementsQueue.Enqueue(insertKey);
            insertedElementsSet.Add(insertKey);

            if (debug)
            {
                IntegrityCheck();
            }
        }

        bTree.Watch();
        for (int i = 0; i < 200000; i++)
        {
            BTreeOperation operation = (BTreeOperation)(random.Next() % 3);

            switch (operation)
            {
                case BTreeOperation.Search:
                    uint searchKey = (uint)random.Next();
                    bTree.Search(searchKey);
                    break;
                case BTreeOperation.Insert:
                    uint insertKey = GetNewKey();
                    bTree.Insert(insertKey, (uint)random.Next());
                    insertedElementsQueue.Enqueue(insertKey);
                    insertedElementsSet.Add(insertKey);
                    break;
                case BTreeOperation.Delete:
                    uint deleteKey = insertedElementsQueue.Dequeue();
                    bTree.Delete(deleteKey);
                    insertedElementsSet.Remove(deleteKey);
                    break;
            }

            if (debug)
            {
                IntegrityCheck();
            }
        }

        Console.WriteLine("Search:");
        bTree.Retrieve(BTreeOperation.Search).Log();
        Console.WriteLine();

        Console.WriteLine("Insert:");
        bTree.Retrieve(BTreeOperation.Insert).Log();
        Console.WriteLine();

        Console.WriteLine("Delete:");
        bTree.Retrieve(BTreeOperation.Delete).Log();
        Console.WriteLine();

        bTree.ResetWatch();
        bTree.Clear();
        insertedElementsQueue.Clear();
        insertedElementsSet.Clear();
    }
    Console.WriteLine();
}

UsageExample(new BPlusStarTree<char, int>(6, 6));

TestBST(new BTree<uint, uint>(6));
TestBST(new BTree<uint, uint>(196));
TestBST(new BStarTree<uint, uint>(6));
TestBST(new BStarTree<uint, uint>(196));
TestBST(new BPlusTree<uint, uint>(6, 6));
TestBST(new BPlusTree<uint, uint>(204, 2048));
TestBST(new BPlusStarTree<uint, uint>(6, 6));
TestBST(new BPlusStarTree<uint, uint>(204, 2048));
