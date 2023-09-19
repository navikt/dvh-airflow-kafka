# Oppsett av ny kafka-konsument
Denne guiden krever følgende verktøy installert: [naisdevice](https://doc.nais.io/device/), [nais-cli](https://doc.nais.io/cli/install/), [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl), [gcloud cli](https://cloud.google.com/sdk/docs/install)

Sette opp en [NAIS applikasjon](https://doc.nais.io/basics/access/?h=kubectl) fra laptop

- Konfigurer [kubectl](https://doc.nais.io/cli/commands/kubeconfig/)
    
- lag en [Nais applikasjon yaml](https://doc.nais.io/persistence/kafka/application/#accessing-topics-from-an-application-on-legacy-infrastructure), eksempel i [dv-a-team-dags](https://github.com/navikt/dv-a-team-dags/blob/main/kafka_cred_prod.yaml)
- Deploy applikasjon til dev/prod
    ```bash
    kubectl apply -f kafka_cred_dev.yaml
    ```
    `aivenapplication.aiven.nais.io/dv-a-team-konsument created`\
    betyr at deploymenten var vellykket.

### Opplasting av kafka kredentials til Google secrets
Siden airflow kjører i knada sitt cluster, må vi hente credentials fra nais-applikasjonen og laste opp dette i en google secret.

1. [dvh-secret-tools](https://github.com/navikt/dvh-secret-tools)
Python cli som henter kafka-hemmeligheter fra en nais app og legger det inn i en google secret. Verktøyet er inspirert av klipp og lim løsningen under, og du unngår å kopiere hemmeligheter i fritekst.

2. Klipp og lim løsning
- Hent credentials til en lokal fil `cred.txt`
    ```bash
    kubectl get secret -n <team-navn> <nais-hemmlighet-navn> -o yaml > cred.txt
    kubectl get secret -n dv-a-team dv-a-team-konsument-cred -o yaml > cred.txt
    ```
    Denne kommandoen laster ned applikasjonshemligher som er `encoded`.
- Decode kafka hemmligheter med hjelp av et [cred_decoding.py](utviklingsmiljo/cred_decoding.py)
- Last opp decoded hemmlighetene på json-format i en google secret. Dette gjør det enkelt å hente disse hemmlighetene og sette som miljøvariabler senere. Følgende variabler skal være i hemmligheten: `KAFKA_BROKERS, KAFKA_CA, KAFKA_CERTIFICATE, KAFKA_CREDSTORE_PASSWORD, KAFKA_PRIVATE_KEY, KAFKA_SCHEMA_REGISTRY, KAFKA_SCHEMA_REGISTRY_PASSWORD, KAFKA_SCHEMA_REGISTRY_USER, KAFKA_SECRET_UPDATED`


### Be om tilgang til aktuelt kafka-topic
Nais applikasjonen vi opprettet i forrige steg har foreløpig ingen tilganger. For at den skal ha tilgang til et topic, må den eksplisit legges til i en ACL(liste som styrer tilganger). Kontakt relevant team, og be om att din NAIS-applikasjon blir lagt til. I vårt eksempel over, er `dv-a-team-konsument` applikasjonsnavnet. Eksempel: [permittering](https://github.com/navikt/permitteringsskjema-api/blob/master/nais/kafka-nav-prod.yaml).\
PS. Dette steget kan gjøres før NAIS-applikasjonen blir opprettet, men du passe på at applikasjonsnavnet i ACL-listen matcher med det navnet du gir til NAIS-applikasjonen.
