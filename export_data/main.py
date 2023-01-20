from export_data.export_bulk_content import fetch_all_content_with_static
from export_data.export_bulk_user import export_all_customer
from export_data.export_ubd import all_ubd_record


def export_data(
        content_with_static: bool = False,
        customer: bool = False,
        ubd: bool = False):
    """
    fetch all data from
    redis and mongodb
    and save to s3
    """
    if content_with_static:

        fetch_all_content_with_static()

    if customer:

        export_all_customer()

    if ubd:
        all_ubd_record()


