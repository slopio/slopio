#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

#define MAX_SOCKETS 256
#define QUIC_MIN_PACKET_LEN 1
#define QUIC_LONG_HEADER_BIT 0x80

struct {
	__uint(type, BPF_MAP_TYPE_REUSEPORT_SOCKARRAY);
	__uint(max_entries, MAX_SOCKETS);
	__type(key, __u32);
	__type(value, __u64);
} sockarray SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, __u32);
} config SEC(".maps");

SEC("sk_reuseport")
int quic_select_reuseport(struct sk_reuseport_md *ctx)
{
	__u8 *data = (__u8 *)(long)ctx->data;
	__u8 *data_end = (__u8 *)(long)ctx->data_end;

	__u32 zero = 0;
	__u32 *num_sockets = bpf_map_lookup_elem(&config, &zero);
	if (!num_sockets || *num_sockets == 0)
		return SK_DROP;

	if (data + 1 > data_end)
		return SK_DROP;

	__u8 first_byte = data[0];
	__u32 index;

	if (first_byte & QUIC_LONG_HEADER_BIT) {
		index = ctx->hash % *num_sockets;
	} else {
		if (data + 2 > data_end)
			return SK_DROP;

		index = data[1];
		if (index >= *num_sockets)
			index = ctx->hash % *num_sockets;
	}

	bpf_sk_select_reuseport(ctx, &sockarray, &index, 0);
	return SK_PASS;
}

char LICENSE[] SEC("license") = "GPL";
