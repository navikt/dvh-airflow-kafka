import base64

def convert_encode_to_decode(file_path):
    with open(file_path, "r") as input_file:
        with open("cred_decoded.txt", "a") as output_file: # decoded values will be added to this file
            for line in input_file:
                if 'KAFKA_' in line:
                    key = line[ : line.index(':') ] # from the begining of the line to : 

                    value_encoded = line[ line.index(':') : ].strip() # the value to decode starts from after : to the end of the line. strip() to delete spaces before and after the value
                    base64_bytes = value_encoded.encode('ascii')
                    message_bytes = base64.b64decode(base64_bytes)
                    value_decoded = message_bytes.decode('ascii')

                    new_line = key + ':' +  value_decoded + '\n\n' # the new line will be key:value. added 2 line shift for readability
                    output_file.write(new_line) # each line will be appended to the output file.

if __name__ == "__main__":
    path = 'cred_encoded.txt' # change to ur file_path
    convert_encode_to_decode(path)
