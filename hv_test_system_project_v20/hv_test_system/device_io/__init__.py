"""Device I/O workers.

These workers ensure that each physical interface (Serial/VISA) is accessed by
exactly one thread, preventing interleaved reads/writes and crossed replies.
"""
