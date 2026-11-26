# Hva felleskonsumenten håndterer utover en enkel Kafka-konsument

## Konklusjon

Denne konsumenten samler mekanismer for batchvis lasting fra Kafka til Oracle: styrt relasting, gjenopptakelse fra lagrede offsets, duplikatkontroll, dataminimering, Oracle-datatyper og rapportering til Airflow. En enkel løkke som leser meldinger og skriver dem til en database dekker ikke dette alene.

Det sterkeste argumentet for gjenbruk er samspillet mellom disse mekanismene. Når samme data må leses igjen etter en feil eller ved relasting, må både leseposisjonen, databaseskrivingen og duplikatkontrollen være riktige. Hvert team som lager sin egen løsning, må implementere og vedlikeholde tilsvarende egenskaper for sine behov.

Dette betyr ikke at alle team bør bruke én felles kjørende konsument. Felles kode og image kan brukes av separate jobber med egne secrets, consumer groups, tabeller og konfigurasjoner. Teamansvar og felles implementasjon er derfor ikke motsetninger.

Koden gir heller ikke en generell garanti for tapsfri eller «exactly-once» lasting. Det finnes begrensninger i blant annet tidsstyrt lesing, delta-beregning og feilbaner. Disse er beskrevet nedenfor.

## Grunnlag og avgrensning

Gjennomgangen gjelder kildekoden, testene, YAML-filene, Docker-oppsettet og dokumentasjonen i dette repoet slik de foreligger 26. november 2026.

Hovedløpet er [oppstart og konfigurasjonslasting](src/main.py#L16), [styring av lastingen](src/mapping.py#L35), [Kafka-kilden](src/kafka_source.py#L48), [transformasjonene](src/transform.py#L134) og [Oracle-målet](src/oracle_target.py#L12). Utviklings-CLI-et er vurdert separat fra selve lastingen.

Her betyr «enkel konsument» en løsning som poller Kafka, dekoder en melding, skriver til databasen og lagrer leseposisjonen. Kafka-klienten håndterer allerede protokoll, partisjoner og mye av forbindelseshåndteringen. Merverdien her ligger i hvordan applikasjonen kobler Kafka til Oracle og Airflow, ikke i at den har erstattet Kafka-klienten.

Dette er en statisk kodegjennomgang. Testene er lest, men ikke kjørt som del av dokumentasjonsarbeidet. Henvisninger til tester viser hvilke scenarioer repoet har tester for, ikke at de er bekreftet grønne nå. Teamspesifikke Airflow-DAG-er, operatører og konfigurasjoner i andre repoer er ikke gjennomgått. Det er heller ikke kontrollert hva som faktisk er deployet.

## Oversikt over problemene som er håndtert

| Utfordring | Hva denne løsningen gjør | Hva en egen konsument må ta stilling til |
|---|---|---|
| Database og Kafka kan ikke committes som én vanlig databasetransaksjon | Skriver og committer Oracle-batchen før synkron offset-commit i det normale `subscribe`-løpet | Rekkefølge, krasj mellom committene og konsekvensene ved omlesing |
| Historiske data må lastes på nytt | `assign` finner start- og slutt-offset per partisjon fra tidsstempler | Hvordan velge og avslutte et avgrenset uttrekk uten å flytte gruppens lagrede posisjon |
| Samme melding kan bli lest flere ganger | Konfigurerbar duplikatkontroll i Oracle | Hva som identifiserer en melding, og hvordan samtidighet håndteres |
| Lastingen skal fortsette fra forrige kjøring | `subscribe` bruker consumer groupens lagrede offsets | Stabil gruppeidentitet og oppførsel når offsets mangler eller er utløpt |
| Databasen skal bestemme startpunktet | `delta` henter høyeste lagrede tidsstempel | Sene meldinger, ulik fremdrift mellom partisjoner og tidssoner |
| Flere partisjoner har ulike grenser | `assign` beregner grenser og avslutter lesing per partisjon | Ikke å behandle topicet som én global offset-sekvens |
| En batch kan være ufullstendig ved avslutning | Restbatch skrives også når den ikke når `batch-size` | Ikke å miste siste del av uttrekket |
| Det oppstår Kafka-feil underveis | Skiller mellom fatale/ikke-gjentakbare feil og gjentakbare feil, og teller feil | Når jobben skal stoppe, hva som allerede er skrevet, og hvordan neste kjøring fortsetter |
| Meldinger har ulike formater og skjemaer | JSON og Avro-dekoding, Avro-oppslag per schema-ID og cache | Format, historiske skjemaer, nøkkeldekoding og nullverdier |
| Store payloads og Unicode skal lagres i Oracle | Konfigurerbare bind-typer, CLOB/BLOB-støtte og regresjonstester | Typeinferens, størrelsesgrenser og driverregresjoner |
| Ikke alle opplysninger skal lagres | Feltfjerning, tilstedeværelsesflagg, meldingsfilter og kode 6/7-oppslag | Hvilke opplysninger som faktisk kan havne i alle målkolonnene |
| Det skal være mulig å undersøke en kjøring | Kafka-metadata, hash, lastetid, logger og XCom-oppsummering | Sporbarhet uten å måtte logge hele payloaden |
| Operatøren må undersøke eller endre leseposisjon | Eget CLI for inspeksjon, uttrekk og kontrollert offset-endring | Utilsiktet commit under feilsøking og endringer mens gruppen er aktiv |

Tabellen beskriver implementerte mekanismer. Forutsetningene og begrensningene under er en del av vurderingen.

## 1. Samspillet mellom databaseskriving og leseposisjon

### Oracle skrives før offset-commit i det normale løpet

I `subscribe`-modus skjer dette:

1. Kafka-kilden samler en batch og gir den til `Mapping`.
2. `Mapping` kaller `OracleTarget.write_batch`.
3. Oracle-målet transformerer meldingene, kjører `executemany` og kaller `con.commit()`.
4. Når kontrollen kommer tilbake til Kafka-generatoren, committes gruppens offsets synkront.

Automatisk Kafka-commit er slått av. Dermed unngår det normale løpet at en bakgrunnscommit markerer meldinger som lest før databasen er ferdig.

Kilder: [Kafka-klientkonfigurasjon](src/kafka_source.py#L165), [Mapping.run_subscribe](src/mapping.py#L42), [batch og offset-commit](src/kafka_source.py#L395), [Oracle-skriving](src/oracle_target.py#L92) og [synkron commit](src/kafka_source.py#L424).

Dette er ikke én atomisk transaksjon på tvers av Kafka og Oracle. Hvis prosessen stopper etter Oracle-commit, men før Kafka-commit, blir batchen lest igjen. Duplikatkontrollen er derfor en nødvendig del av løsningen når målet skal unngå doble rader.

Rekkefølgen over beskriver normalbanen. Den er ikke alene et bevis for korrekt oppførsel ved databasefeil, commit-feil eller ugyldige meldinger. Se begrensningene i del 10.

### Duplikatkontrollen gjør omlesing mulig

`skip-duplicates-with` bygger en `NOT EXISTS`-betingelse i innsettingen. Flere kolonner kombineres med `AND`.

To aktuelle valg har ulik betydning:

| Valg | Hva det identifiserer |
|---|---|
| `kafka_topic`, `kafka_partition`, `kafka_offset` | En bestemt plassering i en Kafka-logg |
| `kafka_hash` | Likhet i byteinnholdet som inngår i hashen |

Topic, partisjon og offset er normalt et bedre utgangspunkt for å unngå at samme Kafka-record lastes to ganger. To forskjellige hendelser kan ha identisk payload. Hash som eneste nøkkel kan da fjerne en hendelse som skulle vært beholdt.

For JSON beregnes hash fra de opprinnelige payload-bytene, før feltfiltrering. For Avro brukes payload-bytene etter Confluent-headeren; schema-ID inngår ikke. Hashen er dermed ikke en generell nøkkel for semantisk like hendelser eller en anonymisering av innholdet.

Kilder: [duplikatkontroll](src/oracle_target.py#L111), [JSON-hash](src/kafka_source.py#L102) og [Avro-hash](src/kafka_source.py#L129).

Duplikatkontrollen er valgfri og er ikke aktiv som standard. `NOT EXISTS` er heller ikke en erstatning for en database-constraint ved samtidige skrivere. Indekser, unikhetskrav og avtalt kjøremønster må håndteres i måldatabasen og jobboppsettet.

## 2. To lastemåter for ulike behov

### `subscribe` fortsetter fra consumer groupens posisjon

Denne modusen passer for gjentatte innlastinger av nye meldinger:

- Konsumenten abonnerer på topicet.
- Kafka fordeler partisjoner gjennom consumer groupen.
- Etter ferdigskrevne batcher lagres gruppens offsets.
- En poll som returnerer `None`, avslutter kjøringen, og eventuell restbatch skrives.

Dette er derfor en avsluttbar batchjobb, ikke en tjeneste som venter ubegrenset på nye meldinger.

`auto.offset.reset` er satt til `earliest`. Hvis gruppen ikke har en gyldig lagret posisjon, kan den starte fra tidligste tilgjengelige data. En ny `group-id` kan derfor føre til omfattende omlesing. Retention kan samtidig ha fjernet eldre data.

Kilde: [read_subscribed_batches](src/kafka_source.py#L366) og [klientkonfigurasjonen](src/kafka_source.py#L165).

### `assign` leser et tidsstyrt uttrekk uavhengig av gruppe-offsets

Dette er mekanismen for historiske uttrekk og relasting:

- `DATA_INTERVAL_START` og `DATA_INTERVAL_END` leses som heltall og brukes i Kafkas tidsoppslag, som forventer millisekunder siden epoch.
- Koden finner start- og slutt-offset separat for hver partisjon.
- Partisjonene tildeles direkte med `assign`.
- Hver partisjon tas ut av lesingen når den har nådd sin beregnede slutt.
- Hvis starttidspunktet ikke har noen treff, utelates partisjonen.
- Hvis sluttidspunktet ikke har noen treff, brukes høyeste watermark minus én.
- Denne modusen committer ikke gruppens offsets.

Dermed kan samme område leses igjen uten å nullstille gruppens fremdrift. Duplikatkontroll avgjør om omlesingen legger til nye rader i en eksisterende måltabell.

Kilder: [intervallvariabler](src/kafka_source.py#L68), [tidsoppslag](src/kafka_source.py#L184), [grenser per partisjon](src/kafka_source.py#L222) og [leseløpet](src/kafka_source.py#L283).

Direkte tildeling fordeler ikke arbeidet mellom jobber slik `subscribe` gjør. To samtidige `assign`-jobber kan lese samme område selv om de har samme gruppe-ID. Jobbplanleggingen må hindre uønsket overlapp.

Tidsgrensene har også viktige forbehold. Dette er ikke et garantert presist halvåpent tidsuttrekk; se del 10.

## 3. Delta fra databasen

Når `target.delta` er satt, leser Oracle-målet `MAX` fra en konfigurert kolonne og bruker resultatet som `DATA_INTERVAL_START`. Tabellen kan angis separat, ellers brukes måltabellen.

Koden tolker databaseverdien som lokal tid i `Europe/Oslo`, konverterer til UTC og deretter til millisekunder. Ved tom tabell blir startverdien `"0"`.

Dette gir et alternativ til å vedlikeholde starttid manuelt. Startpunktet beregnes når Oracle-målet opprettes. For tidsstyrt lesing må `DATA_INTERVAL_END` fortsatt oppgis.

Kilde: [initialisering og delta-beregning](src/oracle_target.py#L17).

Delta er ikke et eget varig offset-register per partisjon. Et globalt makstidsstempel kan ligge foran meldinger i en annen partisjon eller foran senere ankomne meldinger med eldre tidsstempel. Dette er en praktisk mekanisme med forutsetninger, ikke en garanti mot hull.

## 4. Batchgrenser, feil og avslutning

`batch-size` begrenser hvor mange meldinger som samles før databaseskriving. Oracle bruker `executemany`, og hver vellykket batch committes separat. Det begrenser transaksjonsstørrelsen og gjør at hele historikken ikke må ligge i minnet samtidig. Grensen gjelder antall meldinger, ikke antall byte.

Begge strategier har håndtering av restbatcher. Ved en fatal Kafka-feil forsøker kilden å gi videre allerede innsamlede meldinger før feilen stopper kjøringen. Tidligere committede batcher blir ikke rullet tilbake når en senere batch feiler.

Kafka-feil som er fatale eller ikke kan prøves på nytt, stopper jobben. Andre Kafka-feil logges og telles, og polling fortsetter. Dette er ikke en egen retry-kø eller en eksplisitt retry med backoff for hver melding.

I `assign` stopper koden etter ti poll-returer uten melding, for å unngå ubegrenset venting. Telleren nullstilles ikke når en melding mottas. Grensen gjelder derfor ti slike returer samlet i kjøringen, ikke nødvendigvis ti på rad.

Kilder: [assign-løpet og feilbanen](src/kafka_source.py#L283), [subscribe-løpet og avslutningen](src/kafka_source.py#L366) og [Oracle-transaksjonen](src/oracle_target.py#L118).

Repoet har tester for [batcher under, lik og over batchgrensen](src/development/test_integration.py#L132), [fatal feil og videre kjøring](src/development/test_integration.py#L364), [gjentakbar Kafka-feil](src/development/test_integration.py#L414) og [poll-timeout](src/development/test_integration.py#L448).

## 5. Formater, metadata og transformasjoner

### Avro-dekoding bruker meldingens schema-ID

Kilden leser schema-ID fra Confluent-formatet og henter tilhørende skjema fra Schema Registry. Skjemaene caches per ID. Den bruker altså ikke ett hardkodet «nyeste skjema» for alle historiske meldinger.

Dette er nyttig ved relasting av data skrevet med flere skjemaversjoner. Det garanterer ikke at alle historiske skjemaer er kompatible med dagens transformasjon eller måltabell.

JSON dekodes til et objekt. Nøkler kan dekodes som UTF-8 eller et big-endian heltall. `schema: string` finnes også i konfigurasjonen, men har et avvik i ETL-løpet som er omtalt i del 10.

Kilde: [valg av dekoder, nøkkeldekoding og payload-dekoding](src/kafka_source.py#L55).

### Metadata gjør lastingen sporbar

Hver dekodet record får nøkkel, tidsstempel, offset, partisjon og topic. Payload-dekoderne legger til hash og meldingsinnhold, og Avro legger også til schema-ID. YAML-transformasjonen bestemmer hvilke av feltene som faktisk lagres i Oracle.

Transformasjonene støtter blant annet:

- mapping av kildefelt til målkolonner, inkludert nøstede felter;
- tidskonvertering til norsk lokal tid og konvertering til UTF-8-bytes for BLOB;
- konstanter, eksempelvis kildesystem;
- felles lastetid gjennom `$$BATCH_TIME`;
- eksplisitt tillatelse til manglende felt med `allow_undefined`.

`$$BATCH_TIME` settes når `Transform` opprettes. Det er normalt én felles tid for kjøringen, ikke et nytt tidspunkt for hver Kafka-batch.

Kilder: [metadata](src/kafka_source.py#L199), [transformasjonsfunksjoner](src/transform.py#L122), [mappingregler](src/transform.py#L146) og [test av felles lastetid](src/development/test_transform.py#L45).

## 6. Oracle-spesifikke tiltak

Databaseskrivingen bruker bind-variabler for verdiene. `custom-config` kan styre `oracledb.Cursor.setinputsizes`, slik at eksempelvis meldingsinnhold behandles som CLOB eller BLOB og tidsstempel som Oracle TIMESTAMP.

Dette dekker en konkret utfordring som ofte ikke vises i små tester: En kort første melding og en stor senere melding kan gi andre typeinferensproblemer enn en batch med bare korte strenger.

Regresjonstesten skriver en kort verdi først og deretter verdier på 32 byte, 32 KiB minus én, 32 KiB og 1 MiB. Den dekker BLOB, CLOB og NCLOB, bruker flerbyte-Unicode og leser verdiene tilbake fra databasen.

Testkommentarene beskriver problemer ved overgang til Oracle-driver 26 og at oppgraderingen ble trukket tilbake. Avhengigheten er eksplisitt begrenset til `<26.0.0`. Dette er et konkret eksempel på erfaring som er bevart både i versjonsgrensen og i testene.

Kilder: [bind-typer og batchskriving](src/oracle_target.py#L118), [LOB-testene og bakgrunnen](src/development/test_oracle_target.py#L163) og [driverbegrensningen](pyproject.toml#L15).

## 7. Dataminimering og skjerming

Konsumenten har flere konfigurerbare tiltak før lagring:

| Tiltak | Hva koden gjør |
|---|---|
| `message-fields-filter` | Fjerner angitte felt, også i nøstede objekter og lister, før `kafka_message` bygges |
| `flag-field-config` | Erstatter et felt med `1` hvis verdien ikke er `None`, ellers `0` |
| `message-filters` | Beholder payload når minst én regel matcher; ellers settes `kafka_message` til `None` |
| `k6-filter` | Slår opp identifikatorer i Oracle og setter `kafka_message` til `None` ved treff på kode 6/7 |

Flagging betyr tilstedeværelse, ikke sannhetsverdi. En tom streng eller `false` blir også `1`, fordi verdien ikke er `None`.

Meldingsfilter og kode 6/7-filter fjerner ikke hele raden. Metadata og andre felt kan fortsatt transformeres til Oracle-kolonner. De nuller heller ikke automatisk ut personidentifikatorer i Kafka-nøkkelen eller egne målkolonner. Konfigurasjonen må derfor vurderes samlet.

Kode 6/7-oppslaget tar hensyn til gyldighetsdatoer, men bruker datoen fra siste record i batchen for hele oppslaget. Det er ikke en separat historisk skjermingsvurdering på hvert meldingstidspunkt.

Kilder: [feltfjerning og flagging](src/kafka_source.py#L85), [meldingsfilter](src/kafka_source.py#L210), [kode 6/7-oppslag](src/oracle_target.py#L57) og [nulling før transformasjon](src/oracle_target.py#L97).

Testene dekker [nøstet flagging](src/development/test_integration.py#L270), [nøstet feltfjerning](src/development/test_integration.py#L308), [meldingsfilter](src/development/test_kafka_source.py#L43) og [lagring med kode 6/7-filter](src/development/test_oracle_target.py#L93).

## 8. Hva YAML og miljøvariabler faktisk styrer

### Konsumentens YAML-kontrakt

YAML leses med `safe_load`. `source` og `target` valideres med Pydantic-modeller. `transform` bygges separat fra en liste med regler.

| Innstilling | Betydning og forbehold |
|---|---|
| `source.type`, `target.type` | Hovedløpet støtter Kafka til Oracle |
| `source.topic`, `source.group-id` | Velger topic og gruppeidentitet |
| `source.strategy` | `assign` er standard; `subscribe` må velges eksplisitt |
| `source.batch-size` | Antall records før ordinær batchskriving |
| `source.poll-timeout` | Poll-timeout i sekunder; standard er 10 |
| `source.batch-interval` | Markert som utfaset og brukes ikke i leseløpene |
| `source.schema`, `source.key-decoder` | Payload-format og nøkkeldekoding |
| `source.keypath-seperator` | Feltskilletegn for payload-filtreringen; denne stavemåten er API-et |
| `source.message-fields-filter`, `source.flag-field-config`, `source.message-filters` | Tiltakene beskrevet i del 7 |
| `target.table` | Måltabell; opprettes ikke av produksjonsløpet |
| `target.delta` | Databasebasert starttid, ikke et partisjonsvis checkpoint |
| `target.skip-duplicates-with` | Kolonnene for duplikatkontroll; tom liste som standard |
| `target.custom-config` | Blant annet eksplisitte Oracle-bind-typer |
| `target.k6-filter` | Oppslag for kode 6/7 med konfigurerbare feltstier |
| `target.custom-insert` | Finnes i modellen, men brukes ikke av `OracleTarget.write_batch` |
| `transform` | Velger målkolonner, felter, konstanter og konverteringer |

Modellene gir validering av flere typer og obligatoriske felt, men ikke full kontroll av alle feilkonfigurasjoner. For eksempel er ikke positiv `batch-size` eller gyldig rekkefølge på ETL-tidsgrensene uttrykt som slike valideringskrav. En ukjent transformasjonsfunksjon faller tilbake til `identity`, ikke en valideringsfeil.

`custom-config` evaluerer dessuten `value` som et Python-uttrykk. YAML-en må behandles som betrodd programkonfigurasjon, ikke som ubetrodd brukerinput.

Kilder: [YAML og konstruksjon](src/main.py#L16), [konfigurasjonsmodeller](src/config.py#L86), [custom-config](src/base.py#L35) og [valg av transformasjonsfunksjon](src/transform.py#L151).

### Oppstart, secrets og Airflow

| Variabel | Funksjon |
|---|---|
| `CONSUMER_CONFIG_PATH` / `CONSUMER_CONFIG` | Leser YAML fra fil eller miljøvariabel; filsti prioriteres |
| `SOURCE_SECRET_PATH` / `TARGET_SECRET_PATH` | Leser separate Kafka- og Oracle-secrets fra Google Secret Manager |
| `PROJECT_SECRET_PATH` | Alternativ for en samlet secret |
| `DATA_INTERVAL_START` / `DATA_INTERVAL_END` | Tidsgrenser i millisekunder for `assign`; delta kan overstyre start |
| `FAIL_ON_NON_CRITICAL_ERROR` | Med `true` feiler jobben etter fullført lasting hvis `error_count > 0` |
| `XCOM_TARGET_PATH` | Alternativ sti til standardfilen `/airflow/xcom/return.json` |
| `LOG_LEVEL` / `CONSUMER_LOG_LEVEL` | Loggnivå og valg av detaljert feilspor |

Kafka-forbindelsen bruker sertifikat, privatnøkkel og CA via SSL utenfor testmiljø. Secrets leses ved oppstart. Synkronisering av Nais-legitimasjon til Secret Manager og videre rotasjon må håndteres utenfor dette lasteløpet.

XCom inneholder telleverk for hendelser, data, feil og behandling. Det er nyttig for Airflow, men telleren `written_to_db_count` økes med batchlengden, også når duplikatkontrollen hindrer nye rader. `committed_to_producer_count` gjelder consumer-offsets, selv om navnet sier «producer». I `assign` brukes `-1` for denne telleren når kilden har opprettet oppsummeringen.

XCom skrives etter at `Mapping.run()` har returnert. Ved et unntak under lastingen når ikke hovedløpet denne skrivingen. Oppsummeringen er derfor ikke en garantert feilrapport for alle avbrutte kjøringer.

Kilder: [secrets](src/config.py#L11), [TLS](src/kafka_source.py#L165), [oppstart og XCom](src/main.py#L23), [telleverk](src/kafka_source.py#L25) og [feilpolicy](src/main.py#L78).

## 9. YAML for canary, image og CI

### Canary-manifestet er testinfrastruktur

[Canary-manifestet](.nais/kafka-canary-dev.yml) oppretter to `AivenApplication`-ressurser og ett topic i `nav-dev`, i namespacet `dvh-etl-tools`.

| Oppsett | Hva det bidrar med |
|---|---|
| Separate applikasjoner og secrets for produsent og konsument | Skiller skrive- og lesetilgang |
| ACL med henholdsvis `write` og `read` | Gir de to applikasjonene ulike rettigheter til topicet |
| To partisjoner | Gjør det mulig å teste mer enn et enpartisjons-scenario |
| Replikasjonsfaktor 3 og minimum 2 in-sync replicas | Setter brokerkrav; produsentens `acks` må også vurderes for skrivegarantier |
| `cleanupPolicy: delete` og `retentionHours: 168` | En ukes konfigurert retention, ikke et permanent arkiv |

Manifestet deployer ikke selve Python-konsumenten, setter ikke dens gruppe-ID og tester ikke at data når Oracle. Det oppretter Kafka-ressursene som slike tester kan bruke. Kravet om syntetiske testdata står i [oppsettsguiden](kafka-topic.md#L5), ikke som en håndhevet regel i manifestet.

[Deploy-workflowen](.github/workflows/deploy_kafka_canary.yml#L3) bruker `dev-gcp`, har seriell deploy gjennom en concurrency-gruppe og kan startes manuelt. Automatisk push-trigger er begrenset til branchen `dvh-union-kafka` og endringer i manifestet. Guidens formulering om «uansett branch» samsvarer dermed ikke med workflowen.

### Image og tester gir en felles vedlikeholdsflate

[Dockerfile](Dockerfile#L1) bygger med låsefil og `uv sync --frozen --no-dev`, bruker et separat runtime-image og starter `src.main`. [Bygg-workflowen](.github/workflows/build_image.yml#L26) publiserer image og deployer en egen Naisjob.

[Denne Naisjobben](.nais/image-job-template.yml#L1) kjører bare en utskrift og avslutter. Den er ment for image-sporing og sårbarhetsoppfølging, ikke Kafka-innlasting eller en ende-til-ende-canary.

[Test-workflowen](.github/workflows/test.yml#L1) kjører `pytest -v src`. Testoppsettet bruker faktiske Kafka- og Oracle-containere. Det gir mulighet til å avdekke samspillsfeil som en ren mock-test ikke finner. [Dependabot-oppsettet](.github/dependabot.yml#L5) dekker GitHub Actions, uv og Docker.

At disse workflowene finnes, er ikke dokumentasjon på at siste kjøring var vellykket. Image-byggingen har heller ikke en avhengighet til testjobben i den separate test-workflowen.

## 10. Begrensninger som må med i vurderingen av trygg lasting

Disse punktene er funn fra kodelesing, ikke feil som er rettet i denne gjennomgangen.

### Tidsstyrt ETL har ikke samme intervallsemantikk som CLI-et

I ETL-løpet legges meldingen i batchen før sluttgrensen sjekkes. Deretter avsluttes partisjonen ved `offset >= beregnet_slutt`. Recorden ved slutt-offseten blir altså inkludert. Ved sammenhengende tidsvinduer kan det gi overlapp; ved et stort tidsgap kan også en record etter ønsket sluttid komme med.

ETL-løpet kontrollerer heller ikke hvert meldingstidsstempel mot intervallet. Kafka-tidsstempler trenger ikke være sortert med offset, så et tidsoppslag alene garanterer ikke at alle og bare meldinger i tidsvinduet blir lest.

CLI-et bruker derimot eksklusiv slutt-offset og kontrollerer meldingens tidsstempel i tidsuttrekk. Også CLI-et avgrenser først med tidsoppslag og advarer om at tidsuttrekket kan være ufullstendig ved usorterte tidsstempler.

Kilder: [ETL-grensen](src/kafka_source.py#L328), [CLI-grensen og tidskontrollen](src/kafka_cli.py#L570) og [CLI-advarselen](src/kafka_cli.py#L773).

### Globalt delta kan hoppe over data

`MAX(timestamp)` på tvers av partisjoner er ikke det samme som siste ferdigbehandlede offset i hver partisjon. Ved ulik fremdrift eller sene meldinger kan eldre meldinger bli liggende før neste startpunkt. Datoen i Oracle forutsettes også å være norsk lokal tid.

For krav om fullstendig historikk må dere eksplisitt velge mellom partisjonsvise offsets, overlappende uttrekk med duplikatkontroll eller en annen kontrollert innhentingsstrategi. Overlapp med et fast tidsrom dekker heller ikke vilkårlig sene meldinger.

Kilde: [delta-beregningen](src/oracle_target.py#L34).

### Feilbanen beviser ikke tapsfri offset-håndtering

ETL-kilden slår av auto-commit, men ikke automatisk lagring av lokal offset-posisjon. Commit gjøres med `consumer.commit()` uten en eksplisitt liste over siste vellykket skrevne offsets per partisjon.

Ved dekodingsfeil etter en vellykket `poll`, men før meldingen legges i batchen, kan den lokale posisjonen allerede ha passert den ugyldige meldingen. `subscribe` forsøker i `finally` å skrive en tidligere restbatch og deretter committe posisjonen. Denne kombinasjonen gir en risiko for å committe forbi en melding som ikke er lagret.

Dette må verifiseres med en målrettet feiltest før løsningen omtales som tapsfri. Feil under databaseskriving, synkron offset-commit og avslutning av generatoren trenger også egne tester. Den eksisterende testen av fatal feil injiserer en Kafka-feilhendelse; den beviser ikke disse scenarioene.

Kilder: [klientoppsettet](src/kafka_source.py#L165), [dekoding før append og finally-batch](src/kafka_source.py#L392), [commit uten eksplisitte offsets](src/kafka_source.py#L424) og [testen av fatal Kafka-feil](src/development/test_integration.py#L365).

### Dekoderne håndterer ikke alle nullverdier og formater likt

JSON-dekoderen håndterer `None` som payload. Avro- og string-dekoderne gjør ikke tilsvarende i ETL-kilden. String-dekoderen returnerer dessuten en tuple, mens `collect_message` forventer en mapping til `message.update`.

Dette er synlige avvik i koden, ikke en verifisert støtte for tombstones i alle formater eller fungerende `schema: string` gjennom hele ETL-løpet. CLI-et har sin egen dekoding og håndterer null-payload separat.

Kilder: [ETL-dekodere](src/kafka_source.py#L102), [collect_message](src/kafka_source.py#L199) og [CLI-dekoding](src/kafka_cli.py#L406).

### Flere driftsgarantier ligger utenfor løsningen

- Ingen implementert DLQ eller karantene for ugyldige meldinger.
- Ingen egen retry-mekanisme med backoff rundt Oracle-skriving.
- Ingen atomisk transaksjon mellom Oracle og Kafka.
- Ingen automatisk kontroll av at historikk allerede er tapt gjennom retention.
- Ingen automatisk opprettelse av produksjonstabeller, indekser eller unikhetskrav.
- Ingen Prometheus-metrikker, tracing eller alarmregler i det gjennomgåtte lasteløpet.
- Ingen garanti mot samtidige, overlappende relastinger.

Det betyr ikke at slike tiltak mangler i den eksterne Airflow-plattformen. De er ikke dokumentert som egenskaper ved denne implementasjonen.

## 11. CLI-et reduserer risiko ved feilsøking og relasting

[Utviklings-CLI-et](src/kafka_cli.py#L173) er et eget driftsverktøy, ikke en del av den ordinære Oracle-lastingen. Det kan:

- lese records fra bestemte offsets eller tidsintervaller uten å committe;
- vise lagrede gruppe-offsets, watermarks og estimert lag;
- slå opp tidsgrenser per partisjon;
- forhåndsvise nye offsets uten endring;
- endre offsets eksplisitt med `--execute`.

Det slår av både auto-commit og automatisk offset-lagring, bruker direkte tildeling ved uttrekk og slår av automatisk topic-opprettelse. Før en offset-endring krever det at Kafka rapporterer gruppen som `EMPTY` eller `DEAD`. Gruppen må holdes stoppet frem til riktig konsument starter igjen; kontrollen er ikke en lås mot senere oppstart.

Payload skjules som standard i utskriften og vises med `--show-contents`. Nøkkelen vises fortsatt og kan inneholde identifiserende opplysninger. CLI-et bruker heller ikke produksjonens payload-filtre.

Kilder: [klientvern](src/kafka_cli.py#L181), [uttrekk](src/kafka_cli.py#L619), [offset-endring](src/kafka_cli.py#L652) og [dekoding uten payload-filtre](src/kafka_cli.py#L406).

## 12. Hva testene gir grunnlag for å si

| Scenario som finnes i testene | Kilde |
|---|---|
| Flere partisjoner og ulike batchstørrelser i begge strategier | [test_many_messages](src/development/test_integration.py#L134) |
| Flere kjøringer med nye meldinger og uten nye meldinger | [inkrementell konsumering](src/development/test_integration.py#L187) og [uten nye meldinger](src/development/test_integration.py#L216) |
| Fatal Kafka-feil, bevaring av allerede lest batch og videre kjøring | [test_fatal_error](src/development/test_integration.py#L365) |
| Videre polling etter en gjentakbar Kafka-feil | [test_retriable_error](src/development/test_integration.py#L415) |
| Nøstet feltfjerning og flagging | [flagging](src/development/test_integration.py#L271) og [feltfjerning](src/development/test_integration.py#L309) |
| Kode 6/7-oppslag og nulling av payload i Oracle | [Oracle-testene](src/development/test_oracle_target.py#L76) |
| Store LOB-verdier, Unicode og kort første record | [LOB-testen](src/development/test_oracle_target.py#L163) |
| Flere ugyldige konfigurasjoner | [konfigurasjonstestene](src/development/test_config.py#L52) |

De gjentatte kjøringene bruker hash-basert duplikatkontroll fra [testkonfigurasjonen](src/development/conftest.py#L113). De er ikke bevis for alle andre kombinasjoner av duplikatnøkler eller samtidige skrivere.

Særlig viktige manglende scenarioer før sterkere sikkerhetsgarantier er krasj mellom de to committene, databasefeil, commit-feil, ugyldig payload midt i en batch, sene meldinger på tvers av partisjoner, presise tidsgrenser, retention-hull og samtidige relastinger.

## 13. Hva dette betyr for felles eller teamspesifikke konsumenter

For team med samme behov, Kafka til Oracle som rådatainnlasting, er det mye felles atferd å vedlikeholde. Driverbegrensningen og LOB-testene er et konkret eksempel på en feilklasse som ellers må oppdages og løses flere steder.

En fordeling som passer denne løsningen, er:

| Felles ansvar | Teamets ansvar |
|---|---|
| Kafka-lesing, batchskriving og commit-mønster | Eget topic og egen consumer group der lesingen skal være uavhengig |
| Dekodere og Oracle-driverkompatibilitet | Måltabell, indekser, unikhetskrav og rettigheter |
| Mekanismer for relasting og inspeksjon | Hvilket tidsrom som kan lastes på nytt, og hvilket kjøremønster som er tillatt |
| Feltfiltre, transformasjoner og felles regresjonstester | Hvilke data som skal lagres, filtreres eller skjermes |
| Image og vedlikehold av felles kode | Godkjent image-versjon, jobbkonfigurasjon, varsling og oppfølging |

Egen konsument er rimelig når teamet har vesentlig andre krav: domenebehandling per hendelse, andre mål enn Oracle, kontinuerlig lav latenstid, egne transaksjonskrav eller en annen modell for feil og karantene.

Før en egen implementasjon erstatter denne for samme lasteoppgave, bør teamet kunne svare konkret på:

1. Hva skjer ved stopp etter database-commit, men før offset-commit?
2. Hvordan unngås duplikater uten å fjerne legitime hendelser?
3. Hvordan relastes historikk uten hull, utilsiktet overlapp eller endret løpende leseposisjon?
4. Hva skjer med ugyldige meldinger og offsets i samme batch?
5. Hvordan testes store meldinger, historiske skjemaer, flere partisjoner og driveroppgraderinger?
6. Hvilke data kan fortsatt lagres etter filtrering og skjerming?

Begrunnelsen for gjenbruk er at disse spørsmålene går igjen. Denne konsumenten inneholder mye av mekanikken og flere konkrete regresjonstester. Begrensningene viser samtidig hva som må forbedres eller avklares, også når koden er felles.
