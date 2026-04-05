from __future__ import annotations


class ConsoleNotifier:
    def send(self, text: str) -> bool:
        print(text)
        return True
