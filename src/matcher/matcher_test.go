// Package matcher TODO
package matcher

import "testing"

func Test_prepareString(t *testing.T) {
	type args struct {
		p string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "success",
			args: args{
				p: "test_*_",
			},
			want: "test_.*_",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := prepareString(tt.args.p); got != tt.want {
				t.Errorf("prepareString() = %v, want %v", got, tt.want)
			}
		})
	}
}
