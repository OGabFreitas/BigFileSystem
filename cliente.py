import requests
import os

def upload_file(file_path):
    filename = os.path.basename(file_path)
    response = requests.post("http://localhost:5000/upload_request", json={"filename": filename})
    if response.status_code == 200:
        node_url = response.json()["node_url"]
        with open(file_path, 'rb') as f:
            requests.post(f"{node_url}/upload", files={"file": f}, data={"filename": filename})
        print(f"Arquivo '{filename}' enviado para o nó com sucesso.")
    else:
        print("Erro ao obter nó do manager.")

def download_file(filename, destino):
    response = requests.get(f"http://localhost:5000/download_location/{filename}")
    if response.status_code == 200:
        node_url = response.json()["node_url"]
        r = requests.get(f"{node_url}/download/{filename}")
        with open(destino, 'wb') as f:
            f.write(r.content)
        print(f"Arquivo '{filename}' baixado com sucesso para '{destino}'.")
    else:
        print("Arquivo não encontrado no manager.")

def list_files():
    response = requests.get("http://localhost:5000/list")
    arquivos = response.json()
    print("Arquivos disponíveis:")
    for nome, local in arquivos.items():
        print(f"- {nome} @ {local}")

def remove_file(filename):
    response = requests.delete(f"http://localhost:5000/remove/{filename}")
    print(response.text)

if __name__ == "__main__":
    while True:
        comando = input("Comando (ls | rm <arquivo> | cp <origem> <destino> | sair): ").strip()
        if comando == "sair":
            break
        elif comando == "ls":
            list_files()
        elif comando.startswith("rm "):
            _, filename = comando.split(maxsplit=1)
            remove_file(filename)
        elif comando.startswith("cp "):
            partes = comando.split()
            if len(partes) == 3:
                origem, destino = partes[1], partes[2]

                if origem.startswith("remote:"):
                    # baixar do sistema distribuído para o cliente
                    nome_arquivo = origem.replace("remote:", "")
                    download_file(nome_arquivo, destino)
                elif destino.startswith("remote:"):
                    # enviar do cliente para o sistema distribuído
                    if os.path.exists(origem):
                        upload_file(origem)
                    else:
                        print(f"Arquivo local '{origem}' não encontrado para upload.")
                else:
                    print("Para copiar de/para o sistema, use o prefixo remote: em origem ou destino.")
            else:
                print("Uso incorreto de cp. Exemplo: cp origem destino")
        else:
            print("Comando inválido.")