def update_mapping(input_key:dict,input_value,target:dict):
    try:
        target.update({input_key:input_value})
    except:
        target.update({
            input_key:None
        })