#!/usr/bin/env python3
import glob
import os
import re
from dataclasses import dataclass
from typing import List, Set

OUTPUT_DIRECTORY = r"/mnt/c/Users/Srikantan/Downloads/output"
LOG_LINE_RE = re.compile(r"\s*(\d+)\s*->\s*Node:\s*(\d+)\s*=>\s*(ENTER|EXIT)\s*")

@dataclass
class Event:
    timestamp: int
    node_id: int
    ev_type: str


def parse_log_file(path: str) -> List[Event]:
    events: List[Event] = []
    print(f"Parsing {os.path.basename(path)} ...")
    with open(path, "r") as f:
        first_line = f.readline().rstrip("\n")
        print(f"  First entry: {first_line}")
        f.seek(0)
        for lineno, line in enumerate(f, start=1):
            m = LOG_LINE_RE.match(line)
            if not m:
                continue
            ts = int(m.group(1))
            node_id = int(m.group(2))
            ev_type = m.group(3)
            events.append(Event(ts, node_id, ev_type))
    return events

def load_all_events(directory: str) -> List[Event]:
    pattern = os.path.join(directory, "node*.txt")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"no filesin dir {os.path.abspath(directory)}")
        return []
    all_events: List[Event] = []
    for f in files:
        all_events.extend(parse_log_file(f))
    return all_events

def verify_no_violations(events: List[Event]) -> None:
    if not events:
        print("No events found.")
        return
    def event_key(e: Event):
        exit_priority = 0 if e.ev_type == "EXIT" else 1
        return (e.timestamp, exit_priority)
    events.sort(key=event_key)
    active: Set[int] = set()
    isAnyViolating = False
    print("\nChecking if any nodes violated mutex ")
    for ev in events:
        if ev.ev_type == "ENTER":
            if active:
                isAnyViolating = True
                print(f"violation at time {ev.timestamp}: Node {ev.node_id} entered while nodes {sorted(active)} in cs.")
            active.add(ev.node_id)
        elif ev.ev_type == "EXIT":
            if ev.node_id not in active:
                print(f"w at time {ev.timestamp}: Node {ev.node_id} exit but was not recorded as in CS.")
            else:
                active.remove(ev.node_id)

    if isAnyViolating:
        print(" X X X ..mutx violated, some nodes entered CS when other nodes were accessing it.. X X X")
    else:
        print("√ √ √ ..no instance of >1 proc in CS :D..")

def main():
    events = load_all_events(OUTPUT_DIRECTORY)
    verify_no_violations(events)

if __name__ == "__main__":
    main()