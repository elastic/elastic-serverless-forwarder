---
name: Release checklist
about: "Checklist for releasing a new version of Elastic Serverless Forwarder."

---

# Elastic Serverless Forwarder Release Checklist

Version: `vX.Y.Z`

## Version Bump PR

Create a single PR with the following changes:

* [ ] Update version-specific documentation in `docs/reference/` (if needed)
* [ ] Bump version in `share/version.py` to `X.Y.Z`
* [ ] Update `CHANGELOG.md` with all changes for this release
* [ ] Verify linting and tests pass: `make lint && make license && make test`

## Merge and Deployment

* [ ] Review and merge the version bump PR to `main` branch

## Automated Release Process

After merging, verify the automated workflows complete successfully:

* [ ] `create-tag` workflow completes (creates `lambda-vX.Y.Z` tag)
* [ ] `releases-production` workflow completes
  * [ ] Lambda package uploaded to S3 bucket `esf-dependencies`
  * [ ] New version published to AWS Serverless Application Repository (SAR)

## Post-release

* [ ] Create GitHub release for tag `lambda-vX.Y.Z`
  * [ ] Use changelog entries as release notes
* [ ] Verify the new version is available in SAR
* [ ] Announce release in appropriate channels

## Notes

- The `create-tag` workflow triggers automatically when `share/version.py` is merged to `main`
- The `releases-production` workflow triggers automatically after `create-tag` completes
- GitHub release creation is a manual step for release notes and announcements only
