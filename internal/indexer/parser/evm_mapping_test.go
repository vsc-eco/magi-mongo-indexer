package parser_test

import (
	"fmt"
	"testing"

	"github.com/tibfox/magi-mongo-indexer/internal/indexer/mapper"
	"github.com/tibfox/magi-mongo-indexer/internal/indexer/parser"
)

// The EVM bridge's deposit line (account-mapping DEPOSIT-LOG-1) through the
// real events/evm_mapping_mappings.yaml: a row of evm_mapping_deposit_events
// naming the credited account, so a wallet can list the deposit.
func TestEVMMapping_DepositLineToARow(t *testing.T) {
	mf, err := mapper.LoadMappings("../../config/events")
	if err != nil {
		t.Fatal(err)
	}
	const bridge = "vsc1BqoXD32CWWx9UBfh18TnN7Es85RBut2Uhr"
	var found int
	for _, c := range mf.Contracts {
		if c.Address != bridge {
			continue
		}
		for _, m := range c.Events {
			if m.LogType != "map" {
				continue
			}
			found++
			if m.Table != "evm_mapping_deposit_events" {
				t.Fatalf("table %q", m.Table)
			}
			line := "map|t=hive:lordbutterfly|f=0x00000000000000000000000000000000000000a1|a=99850000|as=usdc|bn=21234567|ti=42"
			if !mapper.MatchesLogType(m, line) {
				t.Fatal("the bridge's deposit line does not match the mapping's log type")
			}
			row := parser.ParseLog(m, line)
			want := map[string]string{
				"recipient": "hive:lordbutterfly",
				"sender":    "0x00000000000000000000000000000000000000a1",
				"amount":    "99850000",
				"asset":     "usdc",
			}
			for k, v := range want {
				if got := fmt.Sprint(row[k]); got != v {
					t.Errorf("%s = %q, want %q (row %v)", k, got, v, row)
				}
			}
			if row["l1_block"] != float64(21234567) || row["l1_tx_index"] != float64(42) {
				t.Errorf("l1_block %v, l1_tx_index %v", row["l1_block"], row["l1_tx_index"])
			}
			// A wei amount above 2^53 stays exact as text.
			big := "map|t=hive:a|f=0x00000000000000000000000000000000000000a1|a=12345678901234567891|as=eth|bn=1|ti=0"
			if got := parser.ParseLog(m, big)["amount"]; got != "12345678901234567891" {
				t.Errorf("a large wei amount came out as %v", got)
			}
			// Another log type of the bridge (a withdrawal) is not a deposit row.
			if mapper.MatchesLogType(m, "unmap|n=1|f=hive:a|t=0x00|a=eth|d=1") {
				t.Fatal("an unmap line matched the deposit mapping")
			}
		}
	}
	if found != 1 {
		t.Fatalf("%d deposit mappings for the EVM bridge, want 1", found)
	}
}
