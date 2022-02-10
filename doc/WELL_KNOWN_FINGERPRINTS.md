Well-known fingerprints
=======================

The chunk loader recognizes the fingerprint for 64 KB zero bytes, and
always returns a chunk for that data without fetching it from remote
storage.  There's nothing special about that chunk, but bypassing
remote storage is an important optimisation during tests (some sqlite
tests create large sparse files), and it can't hurt in production.

More generally, the chunk loader has a set of "well known"
fingerprints for which it can return the chunk data even if it's
missing from remote storage.  Handling more fingerprints without
involving remote storage is always compatible with other versions
(chunks are content-addressed, so identical names should have
identical contents).

Once a chunk fingerprint has been handled specially by the chunk
loader for a while, it makes sense to also treat it specially in the
rest of the code.  For example, we can avoid staging (preparing for
upload) chunks when their fingerprint is well known.  However, before
we do that we must make sure the `Copier`'s patrol touch skips this
new well-known chunk: when the patrol touch fails to find a chunk it
wants to keep alive, the `Copier` enters a panic mode and makes sure
the next snapshot is re-created and uploaded from scratch.

Throughout the code, well-known chunk fingerprints like the zero
fingerprint can be used as special placeholders for dummy
fingerprints; the all-zero fingerprint would probably work just as
well, but a dedicated well-known fingerprint can be more explicit.
