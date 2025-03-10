from typing import List


class Database:
    NULL_VALUE = "NULL"

    def __init__(self):
        """Initialize the database with a main store as the base layer."""
        self.stack = [{}]
        self.commands = {
            "SET": self.set_command,
            "GET": self.get_command,
            "UNSET": self.unset_command,
            "COUNTS": self.counts_command,
            "FIND": self.find_command,
            "BEGIN": self.begin_command,
            "ROLLBACK": self.rollback_command,
            "COMMIT": self.commit_command,
        }

    def _get_value(self, key) -> str:
        """Retrieve the value of a key from the transaction stack."""
        return self.stack[-1].get(key, self.NULL_VALUE)

    def set_command(self, args: List[str]) -> None:
        """Set key to value in current transaction."""
        key, value = args
        self.stack[-1][key] = value

    def get_command(self, args: List[str]) -> None:
        """Print the effective value of the key."""
        key = args[0]
        print(self._get_value(key))

    def unset_command(self, args: List[str]) -> None:
        """Mark key as unset in current transaction."""
        key = args[0]
        if self.stack[-1].get(key):
            del self.stack[-1][key]

    def counts_command(self, args: List[str]) -> None:
        """Count keys with the given effective value."""
        value = args[0]
        all_keys = set()
        for tx in self.stack:
            all_keys.update(tx.keys())
        count = sum(1 for key in all_keys if self._get_value(key) == value)
        print(count)

    def find_command(self, args: List[str]) -> None:
        """List keys with the given effective value."""
        value = args[0]
        all_keys = set()
        for tx in self.stack:
            all_keys.update(tx.keys())
        keys = [key for key in all_keys if self._get_value(key) == value]
        print(" ".join(sorted(keys)) if keys else self.NULL_VALUE)

    def begin_command(self, args: List[str]) -> None:
        """Start a new transaction."""
        self.stack.append(self.stack[-1].copy())

    def rollback_command(self, args: List[str]) -> None:
        """Discard the current transaction."""
        if len(self.stack) > 1:
            self.stack.pop()

    def commit_command(self, args: List[str]) -> None:
        """Merge current transaction into parent."""
        if len(self.stack) > 1:
            current = self.stack.pop()
            self.stack[-1].update(current)

    def run(self):
        """Run the interactive console loop."""
        while True:
            try:
                line = input().strip()
                if line == "END":
                    break
                parts = line.split()
                cmd = parts[0]
                args = parts[1:]
                if cmd in self.commands:
                    self.commands[cmd](args)
            except EOFError:
                break


if __name__ == "__main__":
    db = Database()
    db.run()
