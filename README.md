# Eco-Counter Data Import

## Requirements

An Oracle client needs to be installed on the machine this runs on, with configured wallet, tnsnames.ora, and sqlnet.ora. Additionally, a .env file needs to be created, holding variables `USERNAME` and `PASSWORD` for the appropriate Oracle database.

## Error Handling

The program will abort - without logging any error - if it is unable to create/open the lof file.

It will abort (after logging the error) if there is no .env file or the .env file doesn't contained the expected variables.

Otherwise, it will log errors, delete the CSV, and continue running, waiting for the CSV to be re-uploaded to execute. (There are some `.unwrap()`s in threads, but these will propagate up to the main thread and will be handled there.)
