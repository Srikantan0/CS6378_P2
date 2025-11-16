#!/usr/bin/env python3
import argparse
import glob
import os
import re
from dataclasses import dataclass
from typing import List, Set

LOG_LINE_RE = re.compile(r"\s*(\d+)\s*->\s*Node:\s*(\d+)\s*=>\s*(ENTER|EXIT)\s*")

@dataclass
class Event:
    timestamp: int
    node_id: int
    ev_type: str  # "ENTER" or "EXIT"


def parse_log_file(path: str) -> List[Event]:
    events: List[Event] = []
    print(f"Parsing {os.path.basename(path)} ...")
    with open(path, "r") as f:
        for lineno, line in enumerate(f, start=1):
            m = LOG_LINE_RE.match(line)
            if not m:
                # ignore non-matching lines (blank, debug, etc.)
                continue
            ts = int(m.group(1))
            node_id = int(m.group(2))
            ev_type = m.group(3)
            events.append(Event(ts, node_id, ev_type))
    return events


def load_all_events(directory: str) -> List[Event]:
    pattern = os.path.join(directory, "node*.log")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"No node*.log files found in {os.path.abspath(directory)}")
        return []

    print("Found log files:")
    for f in files:
        print("  ", os.path.basename(f))

    all_events: List[Event] = []
    for f in files:
        all_events.extend(parse_log_file(f))

    return all_events


def check_mutual_exclusion(events: List[Event]) -> None:
    if not events:
        print("No events found.")
        return

    # Sort by timestamp; if tie: EXIT before ENTER
    def event_key(e: Event):
        # EXIT (0) before ENTER (1) at same timestamp
        exit_priority = 0 if e.ev_type == "EXIT" else 1
        return (e.timestamp, exit_priority)

    events.sort(key=event_key)

    active: Set[int] = set()
    violation_found = False

    print("\n=== Checking mutual exclusion ===")
    for ev in events:
        if ev.ev_type == "ENTER":
            if active:
                violation_found = True
                print(
                    f"VIOLATION at time {ev.timestamp}: "
                    f"Node {ev.node_id} ENTER while nodes {sorted(active)} already in CS."
                )
            active.add(ev.node_id)
        elif ev.ev_type == "EXIT":
            if ev.node_id not in active:
                print(
                    f"WARNING at time {ev.timestamp}: "
                    f"Node {ev.node_id} EXIT but was not recorded as in CS."
                )
            else:
                active.remove(ev.node_id)

    print("\n=== Result ===")
    if violation_found:
        print("Mutual exclusion VIOLATED.")
    else:
        print("Mutual exclusion PRESERVED (no overlapping critical sections detected).")

    if active:
        print("NOTE: At end of logs, these nodes are still in CS:", sorted(active))


def main():
    parser = argparse.ArgumentParser(
        description="Check mutual exclusion from node*.log files."
    )
    parser.add_argument(
        "directory",
        nargs="?",
        default=".",
        help="Directory containing node*.log files (default: current directory)",
    )
    args = parser.parse_args()

    events = load_all_events(args.directory)
    check_mutual_exclusion(events)


if __name__ == "__main__":
    main()
