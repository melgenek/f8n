package client

import (
	"context"
	"github.com/k3s-io/kine/pkg/endpoint"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test(t *testing.T) {
	ctx := context.Background()
	client, err := New(endpoint.ETCDConfig{
		Endpoints: []string{"http://localhost:2379"},
	})
	require.NoError(t, err)

	//err = client.Put(ctx, "foo1", []byte("bar"))
	////err = client.Update(ctx, "/foo2", 1, []byte("bar"))
	//require.NoError(t, err)
	//
	//_, err = client.Get(ctx, "foo1")
	//require.NoError(t, err)
	//
	//_, err = client.Get(ctx, "foo1")
	//require.NoError(t, err)
	//
	//_, err = client.Get(ctx, "foo1")
	//require.NoError(t, err)

	res, err := client.List(ctx, "foo1", 8)
	require.NoError(t, err)
	require.Len(t, res, 1)
}
