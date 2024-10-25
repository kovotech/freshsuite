import traceback

def format_exception_email(exception:Exception):
    traceback_list = traceback.format_tb(exception.__traceback__)
    tb_str_email = ""
    for i in traceback_list:
        str_tuple = str(i).split(",")
        tb_str_email += f"<br>{str_tuple[0]} in {str_tuple[1]}"
    return f"Exception:{exception.__repr__()}<br>Traceback:{tb_str_email}"
    
def format_exception_logfile(exception:Exception):
    traceback_list = traceback.format_tb(exception.__traceback__)
    tb_str_logfile = ""
    for i in traceback_list:
        str_tuple = str(i).split(",")
        tb_str_logfile += f"\n{str_tuple[0]} in {str_tuple[1]}"
    return f"Exception:{exception.__repr__()}\nTraceback:{tb_str_logfile}"
    

class ApiException(Exception):
    pass

class SchemaCreationError(Exception):
    pass

class MappingError(Exception):
    pass

class InsertStmtError(Exception):
    pass

class DeleteStmtError(Exception):
    pass

class SQLImportError(Exception):
    pass

class SQLEngineError(Exception):
    pass