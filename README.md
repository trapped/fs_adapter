# fs_adapter
Filesystem adapter for `waterlink/active_record`.

`require` it in your code and set `FSDB_PATH` in your ENV (either from the outside, like from the shell, or from the inside: `ENV["FSDB_PATH"] = "/some/folder"`).

## Storage format
### Directory structure
Example structure:

```text
./
`--fsdb.lock              /* locked on open */
`--fsdb/
   `--metadata.lock
   `--metadata            /* db settings */
   `--0_users.lock
   `--0_users/            /* table in the form of id_name */
      `--metadata.lock
      `--metadata         /* table settings (fields) */
      `--0.lock
      `--0                /* single table row */
```

Lockfiles are implemented using `flock(2)` and are shared for reads and exclusive for writes.

### Databases (schemas)
Metadata contains information that cannot be inferred from the directory structure (currently nothing).

### Tables
Contains the table's fields description.

#### Row fields description
Comma-separated list of colon-separated name/type tuples.
Example: `name:string,age:int64`

### Rows
A row is represented by a single file named after the row's id. It contains the
columns' encoded data.

#### Column data encoding
```text
Field = { Int64 | String }
Int64 = binary data
String = length Int64 + binary data
```
