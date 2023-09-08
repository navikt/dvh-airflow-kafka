import base64
import json


def convert_encode_to_decode(file_path):
    with open(file_path, "r") as input_file:
        with open(
            "cred_decoded.txt", "w"
        ) as output_file:  # decoded values will be added to this file
            json_dict = {}

            for line in input_file:
                if "KAFKA_" in line:
                    key = line[
                        : line.index(":")
                    ].strip()  # from the begining of the line to :

                    value_encoded = line[
                        line.index(":") :
                    ].strip()  # the value to decode starts from after : to the end of the line. strip() to delete spaces before and after the value
                    base64_bytes = value_encoded.encode(encoding="utf-8")
                    message_bytes = base64.b64decode(base64_bytes)
                    value_decoded = message_bytes.decode(encoding="utf-8")
                    json_dict[key] = value_decoded

            json_object = json.dumps(json_dict, indent=2)
            output_file.write(json_object)


if __name__ == "__main__":
    path = "cred_encoded.txt"  # change to ur file_path
    convert_encode_to_decode(path)
