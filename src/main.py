import requests

def main():
    url = "https://jsonplaceholder.typicode.com/posts"  # URL da API

    try:
        # Fazer a requisição GET
        response = requests.get(url)

        # Checar o status da resposta
        if response.status_code == 200:
            # Converter a resposta para JSON
            data = response.json()
            # Imprimir os dados
            print("Response:", data)
        else:
            print("GET request not worked. Status code:", response.status_code)

    except Exception as e:
        print("An error occurred:", e)

if __name__ == "__main__":
    main()
