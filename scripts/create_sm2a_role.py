from airflow.www.app import create_app


def create_dag_launcher_role():
    app = create_app()
    with app.app_context():
        security_manager = app.appbuilder.sm
        role_name = "DAG Launcher"

        # Define permissions
        permissions = [
            ("can_read", "My Profile"),
            ("can_create", "DAG Runs"),
            ("can_read", "DAG Runs"),
            ("can_edit", "DAG Runs"),
            ("menu_access", "DAG Runs"),
            ("menu_access", "Browse"),
            ("can_read", "Jobs"),
            ("menu_access", "Jobs"),
            ("can_read", "Task Instances"),
            ("menu_access", "Task Instances"),
            ("can_read", "XComs"),
            ("menu_access", "DAGs"),
            ("menu_access", "Documentation"),
            ("menu_access", "Docs"),
            ("can_read", "DAG Dependencies"),
            ("can_read", "Task Logs"),
            ("can_read", "Website"),
            ("can_edit", "DAG:veda_discover"),
            ("can_read", "DAG:veda_discover"),
            ("can_edit", "DAG:veda_dataset_pipeline"),
            ("can_read", "DAG:veda_dataset_pipeline"),
            ("can_edit", "DAG:veda_collection_pipeline"),
            ("can_read", "DAG:veda_collection_pipeline"),
        ]

        # Check if the role exists, create it if not
        role = security_manager.find_role(role_name)
        if not role:
            role = security_manager.add_role(role_name)

        # Assign permissions to the role
        for perm_name, view_menu_name in permissions:
            # Ensure the menu exists
            security_manager.add_permissions_menu(view_menu_name)

            # Ensure permission exists
            permission = security_manager.get_permission(perm_name, view_menu_name)
            if permission and permission not in role.permissions:
                security_manager.add_permission_to_role(role, permission)

        print(f"Role '{role_name}' created with specified permissions.")


# Execute the script
if __name__ == "__main__":
    create_dag_launcher_role()
