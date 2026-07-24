"""Dicionario de dados da NYC TLC (descricoes de colunas).

Baseado nos data dictionaries oficiais da TLC (trip record user guide):
  - data_dictionary_trip_records_yellow.pdf
  - data_dictionary_trip_records_green.pdf
  - data_dictionary_trip_records_fhv.pdf
  - data_dictionary_trip_records_hvfhs.pdf  (fhvhv)

As descricoes sao aplicadas como COMMENT nas colunas das tabelas Delta
(bronze/silver) para governanca/documentacao no Unity Catalog.
"""

import re

# ---------------------------------------------------------------------------
# Yellow Taxi
# ---------------------------------------------------------------------------
YELLOW = {
    "VendorID": "Codigo do provedor TPEP que gerou o registro. 1=Creative Mobile Technologies; 2=VeriFone Inc.",
    "tpep_pickup_datetime": "Data e hora em que o taximetro foi acionado (inicio da corrida).",
    "tpep_dropoff_datetime": "Data e hora em que o taximetro foi desligado (fim da corrida).",
    "passenger_count": "Numero de passageiros no veiculo (valor informado pelo motorista).",
    "trip_distance": "Distancia da corrida em milhas reportada pelo taximetro.",
    "RatecodeID": "Codigo de tarifa vigente no fim da corrida. 1=Standard; 2=JFK; 3=Newark; 4=Nassau/Westchester; 5=Negociada; 6=Group ride.",
    "store_and_fwd_flag": "Indica se o registro ficou na memoria do veiculo antes do envio (store-and-forward). Y=sim; N=nao.",
    "PULocationID": "TLC Taxi Zone onde o taximetro foi acionado (embarque).",
    "DOLocationID": "TLC Taxi Zone onde o taximetro foi desligado (desembarque).",
    "payment_type": "Forma de pagamento. 1=Cartao de credito; 2=Dinheiro; 3=Sem cobranca; 4=Disputa; 5=Desconhecido; 6=Corrida anulada.",
    "fare_amount": "Tarifa de tempo-e-distancia calculada pelo taximetro.",
    "extra": "Extras e sobretaxas diversas (ex.: US$0,50 e US$1 de horario de pico/noturno).",
    "mta_tax": "Taxa MTA de US$0,50 acionada automaticamente pela tarifa em uso.",
    "tip_amount": "Valor da gorjeta (preenchido automaticamente para cartao; nao inclui gorjetas em dinheiro).",
    "tolls_amount": "Total de pedagios pagos na corrida.",
    "improvement_surcharge": "Sobretaxa de melhoria de US$0,30 cobrada no inicio da corrida (desde 2015).",
    "total_amount": "Valor total cobrado dos passageiros (nao inclui gorjetas em dinheiro).",
    "congestion_surcharge": "Total arrecadado da sobretaxa de congestionamento do estado de NY.",
    "airport_fee": "Taxa de US$1,25 para embarques em LaGuardia e JFK.",
}

# ---------------------------------------------------------------------------
# Green Taxi (mesmas colunas do yellow, com prefixo lpep_ + trip_type/ehail_fee)
# ---------------------------------------------------------------------------
GREEN = {
    "VendorID": "Codigo do provedor LPEP que gerou o registro. 1=Creative Mobile Technologies; 2=VeriFone Inc.",
    "lpep_pickup_datetime": "Data e hora em que o taximetro foi acionado (inicio da corrida).",
    "lpep_dropoff_datetime": "Data e hora em que o taximetro foi desligado (fim da corrida).",
    "passenger_count": "Numero de passageiros no veiculo (valor informado pelo motorista).",
    "trip_distance": "Distancia da corrida em milhas reportada pelo taximetro.",
    "RatecodeID": "Codigo de tarifa vigente no fim da corrida. 1=Standard; 2=JFK; 3=Newark; 4=Nassau/Westchester; 5=Negociada; 6=Group ride.",
    "store_and_fwd_flag": "Indica se o registro ficou na memoria do veiculo antes do envio (store-and-forward). Y=sim; N=nao.",
    "PULocationID": "TLC Taxi Zone onde o taximetro foi acionado (embarque).",
    "DOLocationID": "TLC Taxi Zone onde o taximetro foi desligado (desembarque).",
    "payment_type": "Forma de pagamento. 1=Cartao de credito; 2=Dinheiro; 3=Sem cobranca; 4=Disputa; 5=Desconhecido; 6=Corrida anulada.",
    "fare_amount": "Tarifa de tempo-e-distancia calculada pelo taximetro.",
    "extra": "Extras e sobretaxas diversas (ex.: horario de pico/noturno).",
    "mta_tax": "Taxa MTA de US$0,50 acionada automaticamente pela tarifa em uso.",
    "tip_amount": "Valor da gorjeta (preenchido automaticamente para cartao; nao inclui gorjetas em dinheiro).",
    "tolls_amount": "Total de pedagios pagos na corrida.",
    "ehail_fee": "Taxa de e-hail (chamado eletronico), quando aplicavel.",
    "improvement_surcharge": "Sobretaxa de melhoria de US$0,30 cobrada no inicio da corrida (desde 2015).",
    "total_amount": "Valor total cobrado dos passageiros (nao inclui gorjetas em dinheiro).",
    "payment_type_desc": "Descricao da forma de pagamento.",
    "trip_type": "Tipo de corrida atribuido pela tarifa em uso. 1=Street-hail; 2=Dispatch.",
    "congestion_surcharge": "Total arrecadado da sobretaxa de congestionamento do estado de NY.",
}

# ---------------------------------------------------------------------------
# FHV (For-Hire Vehicle)
# ---------------------------------------------------------------------------
FHV = {
    "dispatching_base_num": "Numero da licenca TLC da base que despachou a corrida.",
    "pickup_datetime": "Data e hora de inicio da corrida.",
    "dropOff_datetime": "Data e hora de fim da corrida.",
    "PUlocationID": "TLC Taxi Zone de embarque.",
    "DOlocationID": "TLC Taxi Zone de desembarque.",
    "SR_Flag": "Indica corridas que fazem parte de uma cadeia de viagem compartilhada (shared ride). 1=compartilhada; nulo caso contrario.",
    "Affiliated_base_number": "Numero da licenca TLC da base afiliada ao veiculo.",
}

# ---------------------------------------------------------------------------
# HVFHS / fhvhv (High-Volume For-Hire Services)
# ---------------------------------------------------------------------------
FHVHV = {
    "hvfhs_license_num": "Numero da licenca TLC da base/empresa HVFHS. HV0002=Juno; HV0003=Uber; HV0004=Via; HV0005=Lyft.",
    "dispatching_base_num": "Numero da licenca TLC da base que despachou a corrida.",
    "originating_base_num": "Numero da licenca TLC da base que recebeu a solicitacao original.",
    "request_datetime": "Data e hora em que o passageiro solicitou a corrida.",
    "on_scene_datetime": "Data e hora em que o motorista chegou ao local de embarque.",
    "pickup_datetime": "Data e hora de inicio da corrida.",
    "dropoff_datetime": "Data e hora de fim da corrida.",
    "PULocationID": "TLC Taxi Zone de embarque.",
    "DOLocationID": "TLC Taxi Zone de desembarque.",
    "trip_miles": "Milhas percorridas com o passageiro.",
    "trip_time": "Tempo total (segundos) da corrida com o passageiro.",
    "base_passenger_fare": "Tarifa base do passageiro, antes de pedagios, gorjetas, impostos e taxas.",
    "tolls": "Total de pedagios pagos na corrida.",
    "bcf": "Black Car Fund arrecadado na corrida.",
    "sales_tax": "Imposto de vendas do estado de NY arrecadado na corrida.",
    "congestion_surcharge": "Sobretaxa de congestionamento do estado de NY arrecadada.",
    "airport_fee": "Taxa de aeroporto para embarque/desembarque em LaGuardia, Newark e JFK.",
    "tips": "Gorjeta paga pelo passageiro.",
    "driver_pay": "Pagamento total ao motorista (sem gorjetas).",
    "shared_request_flag": "Passageiro concordou com corrida compartilhada. Y/N.",
    "shared_match_flag": "Corrida efetivamente compartilhada com outro passageiro. Y/N.",
    "access_a_ride_flag": "Corrida administrada em nome da MTA (Access-A-Ride). Y/N.",
    "wav_request_flag": "Passageiro solicitou veiculo acessivel a cadeira de rodas (WAV). Y/N.",
    "wav_match_flag": "Corrida atendida por veiculo acessivel a cadeira de rodas (WAV). Y/N.",
}

# ---------------------------------------------------------------------------
# Taxi Zone Lookup (dimensao de zonas)
# ---------------------------------------------------------------------------
ZONE_LOOKUP = {
    "location_id": "Identificador da TLC Taxi Zone (1-265). Chave usada em pu_location_id/do_location_id.",
    "borough": "Distrito (borough) ao qual a zona pertence (ex.: Manhattan, Queens).",
    "zone": "Nome da zona (bairro/regiao) da TLC.",
    "service_zone": "Zona de servico: EWR, Boro Zone, Yellow Zone ou Airports.",
}

# ---------------------------------------------------------------------------
# Colunas derivadas/enriquecidas na camada silver
# ---------------------------------------------------------------------------
# Chaves ja em snake_case (o silver padroniza os nomes das colunas).
SILVER_DERIVED = {
    "pickup_year": "Ano do embarque, derivado da coluna de pickup (particao).",
    "pickup_month": "Mes do embarque, derivado da coluna de pickup (particao).",
    "pickup_borough": "Borough de embarque (join com taxi_zone_lookup por pu_location_id).",
    "pickup_zone": "Zona de embarque (join com taxi_zone_lookup por pu_location_id).",
    "pickup_service_zone": "Zona de servico de embarque (join com taxi_zone_lookup).",
    "dropoff_borough": "Borough de desembarque (join com taxi_zone_lookup por do_location_id).",
    "dropoff_zone": "Zona de desembarque (join com taxi_zone_lookup por do_location_id).",
    "dropoff_service_zone": "Zona de servico de desembarque (join com taxi_zone_lookup).",
    "is_airport_trip": "Verdadeiro quando embarque ou desembarque ocorre em zona de aeroporto (service_zone = 'Airports').",
}

# ---------------------------------------------------------------------------
# Mapas codigo -> rotulo, usados para criar colunas *_name no silver
# (yellow/green compartilham vendor/ratecode/payment; fhvhv usa a licenca HVFHS).
# ---------------------------------------------------------------------------
VENDOR_NAMES = {
    1: "Creative Mobile Technologies",
    2: "VeriFone Inc.",
}
RATECODE_NAMES = {
    1: "Standard",
    2: "JFK",
    3: "Newark",
    4: "Nassau/Westchester",
    5: "Negociada",
    6: "Group ride",
}
PAYMENT_TYPE_NAMES = {
    1: "Cartao de credito",
    2: "Dinheiro",
    3: "Sem cobranca",
    4: "Disputa",
    5: "Desconhecido",
    6: "Corrida anulada",
}
HVFHS_LICENSE_NAMES = {
    "HV0002": "Juno",
    "HV0003": "Uber",
    "HV0004": "Via",
    "HV0005": "Lyft",
}

# Descricoes das colunas *_name derivadas dos mapas acima.
SILVER_LABELS = {
    "vendor_name": "Nome do provedor correspondente a vendor_id (1=Creative Mobile Technologies; 2=VeriFone Inc.).",
    "ratecode_name": "Descricao do codigo de tarifa correspondente a ratecode_id (1=Standard; 2=JFK; 3=Newark; 4=Nassau/Westchester; 5=Negociada; 6=Group ride).",
    "payment_type_name": "Descricao da forma de pagamento correspondente a payment_type (1=Cartao de credito; 2=Dinheiro; 3=Sem cobranca; 4=Disputa; 5=Desconhecido; 6=Corrida anulada).",
    "hvfhs_license_name": "Nome da empresa HVFHS correspondente a hvfhs_license_num (HV0002=Juno; HV0003=Uber; HV0004=Via; HV0005=Lyft).",
}

# Colunas Y/N convertidas para boolean no silver (sobrescrevem a descricao base).
SILVER_BOOL_FLAGS = {
    "store_and_fwd_flag": "Registro em store-and-forward (ficou na memoria do veiculo antes do envio), convertido para boolean (Y->true, N->false).",
    "shared_request_flag": "Passageiro concordou com corrida compartilhada, convertido para boolean (Y->true, N->false).",
}

# Mapa por tipo de taxi (usado pelos notebooks bronze).
DICTIONARIES = {
    "yellow": YELLOW,
    "green": GREEN,
    "fhv": FHV,
    "fhvhv": FHVHV,
}


# Nomes irregulares da TLC cuja divisao e ambigua para o algoritmo generico
# (ex.: 'PUlocationID' tem 'PU' como acronimo colado a palavra minuscula).
_SNAKE_OVERRIDES = {
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "PUlocationID": "pu_location_id",
    "DOlocationID": "do_location_id",
}


def to_snake_case(name: str) -> str:
    """Converte um nome de coluna para snake_case.

    Ex.: VendorID->vendor_id, PULocationID->pu_location_id, RatecodeID->ratecode_id,
    dropOff_datetime->drop_off_datetime, SR_Flag->sr_flag.
    """
    if name in _SNAKE_OVERRIDES:
        return _SNAKE_OVERRIDES[name]
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def comments_for(taxi_type: str) -> dict:
    """Retorna o dicionario coluna->descricao para um tipo de taxi (nomes originais)."""
    return DICTIONARIES.get(taxi_type, {})


def silver_comments_for(taxi_type: str) -> dict:
    """Descricoes das colunas do silver: originais em snake_case + derivadas/zonas
    + rotulos (*_name) + flags boolean. Colunas ausentes sao ignoradas ao aplicar
    os comentarios (ver comment_statements)."""
    merged = {to_snake_case(col): desc for col, desc in comments_for(taxi_type).items()}
    merged.update(SILVER_DERIVED)
    merged.update(SILVER_LABELS)
    merged.update(SILVER_BOOL_FLAGS)
    return merged
