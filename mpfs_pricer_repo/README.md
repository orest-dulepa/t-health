**Goal:** 

Take a claim for a professional service (i.e. something done by a medical professional, like a doctor or nurse) and determine how much the US government would pay for that service.

## Terms

- Medicare: The US government’s healthcare program for retired people.
- MPFS: Medicare Physician Fee Schedule, the list of prices that Medicare pays for each professional service
- CPT code or HCPCS code: a 5-digit code that identifies a medical service. For example. `72195` means “MRI scan of the Pelvis, without contrast”
- Modifier or `mod`: Each HCPCS/CPT code can have up to 4 modifiers. Modifiers are 2-digit codes that can change how the code may be billed.
    - Example: `TC` means “Technical component”, which means only the “technical” part of the service was done. An example would be giving someone an X-ray. The “technical” part is creating the x-ray image and the “professional” part is looking at it to decide what the X-ray means.
- Professional Component (PC) and Technical Component (TC): Certain services are split into a professional component and a technical component. These services can be billed independent of one another. Billing technical component only will be reported with a Modifier `TC` on a HCPCS code. Billing Professional component only will be reported with Modifier `26` on a HCPCS code.
- Relative Value Unit (RVU): A value assigned to a CPT code or HCPCS code that indicates the comparative cost of that code compared to all other HCPCS.
    - CMS designates 4 kinds of RVUs per code, three of which are used in payment calculation depending on the place of service:
        - Work RVU (wRVU): The relative value of the work involved in rendering a service
        - Non-Facility Practice Expense RVU (Non-Fac PE RVU): Resource based practice expense for rendering a service in a non-facility setting
        - Facility Practice Expense RVU (Fac PE RVU): Resource based practice expense for rendering a service in a facility setting
        - Malpractice RVU (MP RVU): Relative value for expense incurred for carrying malpractice insurance for a given service
- Geographic Practice Cost Index (GPCI): Establishes a localtion=based value for providing a service based on the cost of being a provider in a given location. There are three types of GPCI per locality:
    - Physician Work GPCI (PW GPCI)
    - Practice Expense GPCI (PE GPCI)
    - Malpractice GPCI (MP GPCI)
- Conversion Factor: The set dollar amount CMS establishes each year to turn RVU and geographic pricing adjustment calculations into actual payment rates in dollars.
    - For 2021, the overall conversion factor is set to $34.8931 but it varies each year, so you always should look it up.
- Place of Service (PoS): Two digit code describing the setting in which care was rendered. For the purposes of this pricer, we need to be concerned with Facility vs. Non-Facility PoS designations. A full listing of Place of Service C odes and their designations can be found [here](https://med.noridianmedicare.com/web/jeb/topics/claim-submission/pos)

## How the price is determined:

- Basic Algorithm
    1. Using the Place of Service code, check if the place where the service was performed counts as a ‘facility’ (i.e. a hospital) or a ‘non-facilty’. The payment rate will be different based on where the service was done.
    2. Look up the Relative Value Unit (RVU) rates (work RVU, PE RVU, MP RVU) that apply, based on the CPT/HCPCS service code, the date the service was performed, and whether a ‘Facility’ or ‘Non-Facility’ PR RVU applies. There are three RVU values:
        - Work RVU (wRVU)
        - Based on location, either the Non-Facility Practice Expense RVU (Non-Fac PE RVU) or Facility Practice Expense RVU (Fac PE RVU)
        - Malpractice RVU (MP RVU)
        - The RVU table also has many other helpful values which are needed to price out different special case adjustments, but they aren’t needed for the basic formula.
    3. Look up the Geographic Practice Cost Index (GPCI) values that apply, based on the specific city where the service was done. There are three values:
        - Physician Work GPCI (PW GPCI)
        - Practice Expense GPCI (PE GPCI)
        - Malpractice GPCI (MP GPCI)
    4. Look up the Conversion Factor that applies for the year the service was performed.
    5. Multiply out the variables to arrive at the base price:
        - Payment Amount = [ (wRVU * PW GPCI) + (Non-Facility/Facility PE RVU * PE GPCI) + (MP RVU * MP GPCI) ] * Conversion Factor
    6. Now that you have the base payment rate, perform any required adjustments.
        - There are many aadjustments that can occur for a given professional claim, those are covered in depth in the next section

## Payment Adjustments To Implement

### Adjustments Based on HCPCS/CPT Code Modifiers

- Modifier AS: Physician Assistant at Surgery
    - First, check the RVU field **Assistant at Surgery** to see the rules for paying the code:
        - 0 = Assistants at surgery are not paid unless supporting documentation is submitted
        - 1 = Assistants at surgery may not be paid
        - 2 = Assistants at surgery may be paid
        - 9 = concept does not apply
    - Action: If payment is possible, payment is calculated as 16% of 85% of MPFS allowed
- Modifiers 80, 81, 82: Assistant at Surgery Services
    - First, check the RVU field **Assistant at Surgery** to see the rules for paying the code:
        - 0 = Assistants at surgery are not paid unless supporting documentation is submitted
        - 1 = Assistants at surgery may not be paid
        - 2 = Assistants at surgery may be paid
        - 9 = concept does not apply
    - Action: If payment is possible, payment is calculated as 16% of MPFS
- Modifier 52, 53: Partially reduced and discontinued services (respectively)
    - Action: Charge amount should reflect the % reduction in services provided. Medicare’s claim processing system will pay the lower of the submitted charge or the allowed amount per the fee schedule
- Modifier 54: Surgical care only.
    - Action: Multiply the MPFS allowable by the sum of pre- and intra-operative percentages
    - The pre- and intra-operative percentage values from from the RVU table
- Modifier 55: Post op care only.
    - Action: Multiply MPFS allowable by post-operative percentage (from RVU table) divided by 90. Multiply result by number of days provider provided post-op care
- Modifier 62: Co-surgeons.
    - First, check the RVU field **Co-Surgeons** to see the rules for paying the code:
        - 0 = Co-Surgeons not permitted
        - 1 = Co-surgeon may be paid if supporting documentation is submitted
        - 2 = Co-surgeons are paid, no documentation is required
        - 9 = Concept does not apply
    - Action: If payable, 62.5% of MPFS allowable
- Modifier QX: CRNA service under supervision of physician.
    - Action: Payment is 50% of MPFS
- Modifier QY: CRNA service under supervision of anesthesiologist
    - Action: Payment is 50% of MPFS
- Modifier 26: Professional Component (PC) only
    - Normal pricing rules should handle this with no extra work.

### Adjustments based on taxonomy code of service provider

- Licensed Clinical Social Worker:
    - Check for: Identification of this provider can be done through Taxonomy Code
    - Action: Payment is made at 75% of MPFS
- Nurse Practitioners and Clinical Nursing Specialist Services
    - Check for: Identification of this provider can be done through Taxonomy Code
    - Action: Payable at 85% of MPFS.
- Nutrition and Dietician Services
    - Check for: Identification of this provider can be done through Taxonomy Code
    - Action: Payable at 85% of MPFS.
- Certified Nurse-Midwife
    - Check for: Identification of Nurse Midwives can be done through Taxonomy Code
    - Action: Payment is made at the lesser of 80% of the actual charge or 100% of MPFS
- Physician Assistant Services
    - Extra check: Can only be paid as long as no facility charges are paid in connection with
             the service.
    - Action: Payment is lesser of 80% of submitted charge or 85% of MPFS

### Adjustments That Only Apply When More Than One Service Is On the Claim

- Multiple procedure adjustments
    - If there multiple line items on the claim or line items with more than one unit on the claim, you need to check the - **Multiple Procedure** value in the RVU table for each code and apply the rules based on the values in the field: 0
        - 0 = No adjustment for multiple procedures (all procedures paid at 100%). Base payment off lower of billed charge amount or fee schedule calculation
        - 2 = If another procedure is reported on the same day, with an indicator of 2 or 3, rank procedures by fee schedule price and apply reduction based on rank (1st 100%, 2nd 50%, 3rd – n 25%). Payment for all procedures is the lower of billed charge or adjusted fee schedule amount.
        - 3 = For endoscopic procedures in the same family (i.e. same base procedure code in Endoscopic Base Code column). Apply the multiple procedure rules for indicator 2 to all procedures with the same base code, then treat the family as a single procedure when ranking against other procedures with the same date of service. If an endoscopic procedure is billed with it’s base code, the base code is never paid as payment for it is included in the more major procedure
        - 4 = When multiple diagnostic imaging codes with TC modifiers are billed from the same family (i.e. the same base procedure code in the diagnostic imaging column), rank procedures based on their fee schedule amount and apply reductions based on rank (1st 100%, 2nd – n 50%). Final payment is lower of billed charge or adjusted fee schedule amount. *There are other reductions here but for the purposes of a first pass, we can build out this part.*
        - 5 = 50% reduction to the practice expense component of certain therapy services
        - 6 = 25% reduction of the second highest and subsequent procedures of diagnostic cardiovascular services (1st 100%, 2nd – n 75%)
        - 7 = Subject to 20% reduction of the second highest and subsequent procedures to the TC of diagnostic ophthalmology services (1st 100%, 2nd – n 80%)
        - 9 = concept does not apply
- Modifier 50, LT, RT: Bilateral surgery
    - A bilateral surgery is one where the surgery might be done on both sides of the body at one time (like replacing both hips). In these cases, doctors usally get paid less than doing the same surgery twice.
    - Check if modifier 50 used, LT and RT are used on the same code, or any codes have a unit of 2. If so, you need to check the **Bilateral Surgery** value in the RVU table for each code and apply the rules based on the values in the field:
        - 0 = payment adjustment for bilateral surgery is not appropriate. If a code is billed with modifier 50, 2 units or on two lines with LT and RT modifiers, payment is calculated at the lower of the aggregate submitted charge or 100% of the fee schedule amount of one unit of the code
        - 1 = payment adjustment applies if code is billed with modifier 50, 2 units, or on two lines with LT and RT modifiers. Final payment is the lower of the total submitted charge or 150% of the fee schedule amount for a single code. If a bilateral procedure occurs on the same day as other procedures, apply the bilateral adjustment before applying any multiple procedure rules
        - 2 = payment adjustment does not apply. If procedure is reported as bilateral (2 units, RT + LT or modifier 50), base payment on lower of submitted charge for both sides or 100% of the fee schedule amount
        - 3 = payment adjustment does not apply. If procedures is reported as bilateral (2 units, RT + LT or modifier 50), base payment for **each** side on the lower of the actual charge for **each** side or 100% of the fee schedule amount for **each** Determine this calculation before applying any multiple procedure reductions
        - 9 = concept does not apply

## Reference

### Variables in the RVU table that you will need for calculating adjustments

- **PC/TC Indicator**:
    - 0 = Physician services that cannot be split into Professional and Technical components
    - 1 = Diagnostic tests for radiology services; can be split into Professional and Technical components. Modifiers 26 and TC are appropriate with this code
    - 2 = Professional component only codes, modifiers 26 and TC not appropriate
    - 3 = Technical component only codes, modifiers 26 and TC not appropriate
    - 4 = Global test only codes where there are associated codes that describe the professional and technical components
    - 5 = Incident to codes that are covered when provided by auxillary personell under physician supervision. Not payable when occurring in IP or OP hospital settings (PoS Codes: 19, 21, 22, 23)
    - 6 = Laboratory Physician interpretation codes where separate payment is made for physician interpretation. Performing the test is paid through separate means
    - 7 = Phyisical therapy service for which payment may not be made
    - 8 = Physician interpretation codes for clinical laboratory where payment may be made if the doctor analyzes and abnormal smear (CPTs 88414,85060, P3001-26). No TC billing is recognized. No payment is made if patient is in an OP hospital or non-hospital patient, payment is made through clinical lab fee schedule
    - 9 = concept of professional/ technical component does not apply
- **Global Surgery**: Provides time frame that apply to each surgery where no other payment will be made for related services
    - 000 = Endoscopic or minor procedure, payment for evaluation and management (EM) services on the day of surgery generally not payable
    - 010 = Minor procedure where payment for EM services cannot be made the day before surgery or the 10 days following
    - 090 = Major procedure where payment for EM services cannot be made the day before surgery or the 90 days following
    - MMM = Maternity codes, global period does not apply
    - XXX = Global concept does not apply
    - YYY = Medicare administrator determines whether global concept applies and establishes post-operative period at time of pricing
    - ZZZ = Code is related to another service and is bundled with global period of that other service
- **Preoperative percentage**
    - Used in various adjustments
- **Intraoperative percentage**
    - Used in various adjustments
- **Postoperative percentage**
    - Used in various adjustments
- **Multiple Procedure (Modifier 51)**
    - 0 = No adjustment for multiple procedures (all procedures paid at 100%). Base payment off lower of billed charge amount or fee schedule calculation
    - 2 = If another procedure is reported on the same day, with an indicator of 2 or 3, rank procedures by fee schedule price and apply reduction based on rank (1st 100%, 2nd 50%, 3rd – n 25%). Payment for all procedures is the lower of billed charge or adjusted fee schedule amount.
    - 3 = For endoscopic procedures in the same family (i.e. same base procedure code in Endoscopic Base Code column). Apply the multiple procedure rules for indicator 2 to all procedures with the same base code, then treat the family as a single procedure when ranking against other procedures with the same date of service. If an endoscopic procedure is billed with it’s base code, the base code is never paid as payment for it is included in the more major procedure
    - 4 = When multiple diagnostic imaging codes with TC modifiers are billed from the same family (i.e. the same base procedure code in the diagnostic imaging column), rank procedures based on their fee schedule amount and apply reductions based on rank (1st 100%, 2nd – n 50%). Final payment is lower of billed charge or adjusted fee schedule amount. *There are other reductions here but for the purposes of a first pass, we can build out this part.*
    - 5 = 50% reduction to the practice expense component of certain therapy services
    - 6 = 25% reduction of the second highest and subsequent procedures of diagnostic cardiovascular services (1st 100%, 2nd – n 75%)
    - 7 = Subject to 20% reduction of the second highest and subsequent procedures to the TC of diagnostic ophthalmology services (1st 100%, 2nd – n 80%)
    - 9 = concept does not apply
- **Bilateral Surgery**
    - 0 = payment adjustment for bilateral surgery is not appropriate. If a code is billed with modifier 50, 2 units or on two lines with LT and RT modifiers, payment is calculated at the lower of the aggregate submitted charge or 100% of the fee schedule amount of one unit of the code
    - 1 = payment adjustment applies if code is billed with modifier 50, 2 units, or on two lines with LT and RT modifiers. Final payment is the lower of the total submitted charge or 150% of the fee schedule amount for a single code. If a bilateral procedure occurs on the same day as other procedures, apply the bilateral adjustment before applying any multiple procedure rules
    - 2 = payment adjustment does not apply. If procedure is reported as bilateral (2 units, RT + LT or modifier 50), base payment on lower of submitted charge for both sides or 100% of the fee schedule amount
    - 3 = payment adjustment does not apply. If procedures is reported as bilateral (2 units, RT + LT or modifier 50), base payment for **each** side on the lower of the actual charge for **each** side or 100% of the fee schedule amount for **each** Determine this calculation before applying any multiple procedure reductions
    - 9 = concept does not apply
- **Assistant at Surgery**
    - 0 = Assistants at surgery are not paid unless supporting documentation is submitted
    - 1 = Assistants at surgery may not be paid
    - 2 = Assistants at surgery may be paid
    - 9 = concept does not apply
- **Co-Surgeons**: Indicates services for which two surgeons in separate specialties may be paid (Modifier 52)
    - 0 = Co-Surgeons not permitted
    - 1 = Co-surgeon may be paid if supporting documentation is submitted
    - 2 = Co-surgeons are paid, no documentation is required
    - 9 = Concept does not apply
- **Team Surgery** (Modifier 66)
    - 0 = Team surgeons not permitted/ paid
    - 1 = Team surgeons may be paid with documentation submission
    - 2 = Team surgeons permitted
    - 9 = Concept does not apply
- **Endoscopic base code**: CPT/HCPCS code that identifies endoscopic base code for a given code with a multiple procedure indicator of 3

## Development

### Running dev version with docker

#### Docker requirements

*docker* v18.06.0+ and *docker-compose* are required

#### Start docker containers

```bash
docker-compose up -d --build
```

#### Stop docker containers

```bash
docker-compose down
```

### Running dev version in PyCharm

Use **Docker** or **Flask (restapi.app)** predefined configurations to run the app

### Configuration: 
Create *.env* file based on *.env.template*

In production mode set AIRBRAKE_PROJECT_ID and AIRBRAKE_PROJECT_KEY to enable integration with airbrake.
For development mode leave AIRBRAKE_PROJECT_* env variables empty.

Rest API is available at [http://localhost:5000](http://localhost:5000)

Celery and redis are running as background processes


### Dev without Docker

#### Redis

Redis should be installed and running at **redis://localhost:6379** 

#### Run Web Service
```bash
PYTHONPATH=. python restapi/app.py
```

#### Run Celery Worker
```bash
celery -A worker.tasks worker -c 1 --loglevel=INFO -Q mpfs
```

### Postman

Import [postman collection](mpfs_pricer.postman_collection.json) 

Update variable **base_url** from **http://localhost:5000** to your web server valid address if needed

+ Run **price_claim** POST method with your body request to start process
+ Run **price_claim** GET method to receive result of previous **price_claim** call
+ Run **price_claim?immediately=true** POST method with your body request to receive result immediately

### Production

#### Running as systemd services on default AWS Ubuntu 20.04 instance

Clone the repo at `/home/ubuntu/`

Install `./systemd_config/*` to create the services:

```
sudo cp * /etc/systemd/system/
sudo systemctl start mpfs_rest_api
sudo systemctl enable mpfs_rest_api
sudo systemctl start mpfs_celery_worker
sudo systemctl enable mpfs_celery_worker
```

Check that they are running:
```
sudo systemctl status mpfs_rest_api
sudo systemctl status mpfs_celery_worker
``
```

Enable nginx forwarding to the socket:

```
sudo nano /etc/nginx/sites-enabled/default
```

```
        location /mpfs/ {
           rewrite  ^/mpfs/(.*) /$1 break;
           include uwsgi_params;
           uwsgi_pass unix:/home/ubuntu/mpfs_pricer/mpfs_pricer_service.sock;
        }
```

```
sudo systemctl reload nginx
```