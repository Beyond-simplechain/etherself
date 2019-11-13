package trie

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"testing"
)

func TestTrie(t *testing.T) {

	const memonly = true
	diskdb := ethdb.NewMemDatabase()
	triedb := NewDatabase(diskdb)

	trie, _ := New(common.Hash{}, triedb)
	//
	//updateString(trie, "123456", "asdfasdfasdfasdfasdfasdfasdfasdf")
	root, _ := trie.Commit(nil)
	if !memonly {
		triedb.Commit(root, true)
	}

	fmt.Println("empty root", root.String())

	//insert key:1,value:a
	key, val := []byte("1"), []byte("a")
	trie.Update(key, val)
	//① key => hexKey=[3,1,16], val => valueNode=[97]
	//② ∵t.root=nil ∴return shortNodeA(hexKey,valueNode,newFlag)
	//③ t.root = 返回的shortNodeA(...)

	fmt.Println("uncommit root", root.String(), "emptyNode")
	root, _ = trie.Commit(nil)
	fmt.Println("commited root", root.String(), "newNode")

	//insert key:2,value:b
	key2, val2 := []byte("2"), []byte("b")
	trie.Update(key2, val2)
	//① key => hexKey=[3,2,16], val => valueNode=[98], root=shortNodeA
	//② 是shortNode, prefix=[3]
	//③ 创建fullNodeA: children[1]=shortNodeB1(key:[1,16],val:[97],)
	//				   children[2]=shortNodeB2(key:[2,16],val:[98],)
	//④ 创建前缀的shortNodeB(key:[3], val:fullNodeA[,shortNodeB1,shortNodeB2,...], dirty)

	root, _ = trie.Commit(nil)
	fmt.Println("commited root", root.String(), "newNode2")

	//insert key:22,value:c
	key3, val3 := []byte("22"), []byte("c")
	trie.Update(key3, val3)
	//① key => hexKey=[3,2,3,2,16], val => valueNode=[99], root=shortNodeB([3]:fullNodeA)
	//②

	root, _ = trie.Commit(nil)
	fmt.Println("commited root", root.String(), "newNode3")
}
