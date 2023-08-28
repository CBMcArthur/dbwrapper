# dbwrapper



## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

## Integrate with your tools

- [ ] [Set up project integrations](https://gitlab.com/cbmcarthur/dbwrapper/-/settings/integrations)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing(SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

## Design and Planning
- [ ] Define the features and functionality you want in your wrapper.
- [ ] Consider features like connection pooling, schema management, backup and restore, query building, and error handling.
- [ ] Create a clear and user-friendly API for your wrapper.

## Testing and Documentation
- [ ] Thoroughly test db_wrapper with various scenarios and edge cases.
- [ ] Document the package's usage with clear examples and API documentation.

## Packaging and Distribution
- [ ] Package your Python wrapper using tools like setuptools for easy installation.
- [ ] Consider publishing it on PyPI if you want to share it with the community.

## Integration
- [ ] Integrate your wrapper into your database backup script or any other projects where it can be useful.

## DB Wrapper
**Objective**: Develop a Python package that acts as a wrapper for PostgreSQL database operations, providing enhanced features and ease of use. 

## Description
**Database Connection**:
- Uses SQLAlchemy for connection management and connection pooling

**Schema Management**:
- Add functions for creating, updating, and managing database schemas.
- Include support for migrations if needed.

**Backup and Restore**:
- Enhance your backup and restore functions, including options for full and partial backups.
- Implement data validation during restores.

**Query Building**:
- Create a query builder to simplify the construction of SQL queries.
- Support parameterized queries to prevent SQL injection.

**Error Handling and Logging**:
- Implement robust error handling to provide meaningful error messages to users.
- Set up logging to track database operations and errors.

## Project status
This will be a work in progress among other works in progress as time allows to practice some development work 
