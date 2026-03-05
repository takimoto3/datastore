package testsuite

import (
	"context"
	"errors"
	"testing"

	"go.mercari.io/datastore/v2"
)

func transactionCommit(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	var key datastore.Key
	{ // Put
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		key = client.IncompleteKey("Data", nil)
		pK, err := tx.Put(key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		c, err := tx.Commit()
		if err != nil {
			t.Fatal(err)
		}

		key = c.Key(pK)
		if v := key.ID(); v == 0 {
			t.Errorf("unexpected: %v", v)
		}
	}
	{ // Get
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = tx.Get(key, obj)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Delete
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}

		err = client.Get(ctx, key, &Data{})
		if err != datastore.ErrNoSuchEntity {
			t.Errorf("unexpected: %v", err)
		}
	}
}

func transactionRollback(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	key := client.NameKey("Data", "test", nil)

	{ // Put
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.Put(key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		err = client.Get(ctx, key, &Data{})
		if err != datastore.ErrNoSuchEntity {
			t.Errorf("unexpected: %v", err)
		}
	}
	{ // Delete
		_, err := client.Put(ctx, key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		err = client.Get(ctx, key, &Data{})
		if err != nil {
			t.Errorf("unexpected: %v", err)
		}
	}
}

func transactionJoinAncesterQuery(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	parentKey := client.NameKey("Parent", "p", nil)
	key := client.NameKey("Data", "d", parentKey)

	_, err := client.Put(ctx, key, &Data{Str: "Test"})
	if err != nil {
		t.Fatal(err)
	}

	tx1, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()

	tx2, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	q := client.NewQuery("Data").Transaction(tx1).Ancestor(parentKey)
	var list1 []*Data
	_, err = client.GetAll(ctx, q, &list1)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(list1); v != 1 {
		t.Fatalf("unexpected: %v", err)
	}
	obj1 := list1[0]

	obj2 := &Data{}
	err = tx2.Get(key, obj2)
	if err != nil {
		t.Fatal(err)
	}

	obj1.Str = "Test1"
	obj2.Str = "Test2"

	_, err = tx1.Put(key, obj1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx2.Put(key, obj2)
	if err != nil {
		t.Fatal(err)
	}

	_, err2 := tx2.Commit()
	_, err1 := tx1.Commit()

	// In Cloud Datastore (Optimistic), tx2 usually succeeds and tx1 fails.
	// In Firestore Emulator (Pessimistic), tx2 might fail first if tx1 has already "touched" the data.
	// The core requirement is that one succeeds and the other fails with ErrConcurrentTransaction.
	if (err1 == nil && err2 == datastore.ErrConcurrentTransaction) ||
		(err2 == nil && err1 == datastore.ErrConcurrentTransaction) {
		// Success: a conflict was detected and handled correctly.
	} else {
		t.Fatalf("expected one transaction to succeed and the other to fail with concurrent transaction error. got tx1: %v, tx2: %v", err1, err2)
	}
}

func transactionCommitAndRollback(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Rollback()
	if err == nil || err.Error() != "datastore: transaction expired" {
		t.Fatal(err)
	}

	_, err = tx.Commit()
	if err == nil || err.Error() != "datastore: transaction expired" {
		t.Fatal(err)
	}
}

func runInTransactionCommit(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	var pK datastore.PendingKey
	c, err := client.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		key := client.IncompleteKey("Data", nil)
		var err error
		pK, err = tx.Put(key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	key := c.Key(pK)
	if v := key.ID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}
}

func runInTransactionRollback(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	_, err := client.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		key := client.IncompleteKey("Data", nil)
		_, err := tx.Put(key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		return errors.New("this tx should failure")
	})
	if err == nil {
		t.Fatal(err)
	}
}
