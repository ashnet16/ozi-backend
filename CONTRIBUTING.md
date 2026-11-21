# Contributing to Ozi

Thanks for helping improve Ozi. Bug reports, fixes, features and documentation are all welcome.

## How to contribute

1. Open an issue describing the bug or the change, unless it is small.
2. Fork the repository and create a branch from `main`.
3. Make your change. Follow the setup and linting steps in the [Developer Guide](readme.md#developer-guide).
4. Sign off every commit (see below).
5. Add a line describing your change under **Unreleased** in [CHANGELOG.md](CHANGELOG.md).
6. Open a pull request that explains what changed and how you tested it.

## Developer Certificate of Origin

Ozi uses the [Developer Certificate of Origin](https://developercertificate.org) (DCO) instead of a contributor license agreement. By signing off a commit, you certify that you wrote the change, or otherwise have the right to submit it, under the project's license. You keep the copyright to your contribution.

To sign off, add a `Signed-off-by` line with your real name and email to each commit message:

```
Signed-off-by: Jane Doe <jane@example.com>
```

Git adds it for you with `-s`:

```bash
git commit -s -m "Fix reaction join for recasts"
```

To sign off commits you have already made on your branch:

```bash
git rebase --signoff main
git push --force-with-lease
```

Pull requests with unsigned commits cannot be merged.

## License

By contributing, you agree that your contributions are licensed under the [GNU Affero General Public License v3.0](LICENSE), the same license as the rest of Ozi.
