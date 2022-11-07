<!-- Thank you for contributing to TiKV Migration Toolset!

PR Title Format: "[close/to/fix #issue_number] summary" -->

### What problem does this PR solve?

Issue Number: close #issue_number

Problem Description: **TBD**

### What is changed and how does it work?


### Code changes

<!-- REMOVE the items that are not applicable -->
- Has exported function/method change
- Has exported variable/fields change
- Has interface methods change
- Has persistent data change
- No code

### Check List for Tests

This PR has been tested by at least one of the following methods:
- Unit test
- Integration test
- Manual test (add detailed scripts or steps below)
- No code

### Side effects

<!-- REMOVE the items that are not applicable -->
- Possible performance regression, WHY: **TBD**
- Increased code complexity, WHY: **TBD**
- Breaking backward compatibility, WHY: **TBD**
- No side effects

### Related changes

<!-- REMOVE the items that are not applicable -->
- Need to cherry-pick to the release branch
- Need to update the documentation
- Need to be included in the release note
- No related changes

### To reviewers

<!-- Please keep the following text as a reminder to PR reviewers -->
Please follow these principles to check this pull requests:

- **Concentration**. One pull request should only do one thing. No matter how small it is, the change does exactly one thing and gets it right. Don't mix other changes into it.
- **Tests**. A pull request should be test covered, whether the tests are unit tests, integration tests, or end-to-end tests. Tests should be sufficient, correct and don't slow down the CI pipeline largely.
- **Functionality**. The pull request should implement what the author intends to do and fit well in the existing code base, resolve a real problem for TiDB/TiKV users. To get the author's intention and the pull request design, you could follow the discussions in the corresponding GitHub issue or [internal.tidb.io](https://internals.tidb.io) topic.
- **Style**. Code in the pull request should follow common programming style. *(For Go, we have style checkers in CI)*. However, sometimes the existing code is inconsistent with the style guide, you should maintain consistency with the existing code or file a new issue to fix the existing code style first.
- **Documentation**. If a pull request changes how users build, test, interact with, or release code, you must check whether it also updates the related documentation such as READMEs and any generated reference docs. Similarly, if a pull request deletes or deprecates code, you must check whether or not the corresponding documentation should also be deleted.
- **Performance**. If you find the pull request may affect performance, you could ask the author to provide a benchmark result.

*(The above text mainly refers to [TiDB Development Guide](https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/review-a-pr.html#checking-pull-requests). It's also highly recommended to read about [Writing code review comments](https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/review-a-pr.html#writing-code-review-comments))*
