import json
from worker import worker
import os
import uuid
from conv_handleing import agents_conv_history, inserting_agent_chat_buffer, monolog, get_best_worker_response
from conv_to_pdf import conversation_to_pdf

limit_subquestions = 3

director_system_prompt = """
#Role :
You are an ESG consultant with 10-years of experience in Environment, Social and Governance topics in context of India's BRSR standards and International GRI standards. Your task is to first break down the user's prompt and understand what is being asked, and then extract the exact information from the Questions and answers provided to you and respond with information requested by the user. With great efforts, prioritise accuracy and respond with an answer generated only using information available only from the conversations history provided to you.
	
##Response format :
When responding, give a summary of the requested information in natural language first. So that the user understands the information in a better manner from the first paragraph of your response. And follow this paragraph with well-structured bullet points or a table/matrix of the most important quantitative information you found. At the end of your response, always give a short summary of your response. And finally, always ask users if they need more help in analytically-relevant topics around what they asked; by suggesting them some questions if they would like to ask as follow ups.

##Additional information :
If the information requested by a user is not found, try to give information that seems relevant/close to what the user requested, but stick to the information from the Questions and answers you have. And in such cases, suggest users to ask questions around information you have available in the current knowledge base but be sure that it is relevant to what the user asked originally. 

Currently there are only following companies available to the worker agent in the Database:
1. Hindustan Petroleum Corporation Limited (HPCL)
2. Indian Oil Corporation Limited (IOCL)

"""

manager_system_prompt = f"""
#Role:
You are an ESG Specialist with 10 Years of Experience in Sustainability consulting, BRSR reporting, XBRL reporting, sustainability reporting, GRI guidelines etc.
As an expert in ESG consulting, you know what information is generally available inside the XBRL Datasheets; Indian BRSR and Sustainability Reports; and also in global GRI-standard sustainability reports.You need to break down the user prompt into sub-questions, each sub-query must only ask about a single company. When you break down the user query into subqueries, you should keep in mind your knowledge about information inside these reports. That will help you make questions in a manner that it helps worker agents to where such information could be present as well. You are allowed to only create up to a maximum of only {limit_subquestions} most relevant subqueries from the user query. The relevancy of your subqueries should be directly related to create an answer to the user query.
Also remember that the worker agent will probably have access to the XBRL Datasheets, BRSR Reports and Sustainability reports inside its database. So based on your knowledge in ESG, you can guide the worker to look into the most relevant places.
While creating subqueries, you also need to think about the point of view of the user and why he must be asking that query. It will help you think of relevant information around the user query but that was not specifically asked by the user. For example: if the user asked for materiality assessment, you know that the user might get help in also knowing materiality assessments of that sector’s company from international GRI or Sustainability reports. Because this could additionally help the user get additional contexts from global standards as well. So think and add such subqueries in the list as well.

BRSR Reports generally contains: 
Details of the Entity, like corporate identity, registered contact details etc
Details of Products/Services like Description of the main business activities and details of products/services sold and Breakdown of turnover contribution by key product/service categories.
Details on operational locations (number of plants and offices, both national and international). Markets served including export contribution and a brief on customer segmentation (retail and bulk customers).
-Employee and worker statistics (permanent and non-permanent)
-Diversity and grievance redressal mechanisms for staff.
Holding, Subsidiary and Associate Companies (including Joint Ventures), -Lists of associated companies along with the percentage share held and details on whether they participate in business responsibility initiatives.
CSR Details, -Information on the applicability of CSR, -Turnover and net worth figures relevant to CSR considerations.
Transparency and Disclosures Compliances, -Grievance redressal mechanisms for various stakeholder groups (communities, investors, employees, customers, and value chain partners), -Data on the number of complaints filed and resolved, along with web links to grievance policies.
Additional Disclosures on Governance and Leadership, -A director’s statement highlighting ESG challenges, targets, and sustainability initiatives (including decarbonisation efforts, transition to green energy, and community engagement).
General Disclosures, -Provides the company’s core details (incorporation, addresses, financial year, stock exchange listings, paid‐up capital, etc.), -Outlines details on products/services, business activities, and operational information (such as plant locations and market reach), -Contains employee and operational statistics similar to disclosures.
Management and Process Disclosures, -Describes the policies and procedures adopted to implement the company’s sustainability framework, -Includes details of codes of conduct, whistleblower and grievance redressal policies, and stakeholder engagement mechanisms, -Outlines the internal review processes and the assurance methodology (with an independent assurance statement provided by Bureau Veritas).
Principle Wise Performance Disclosure, -The section details performance indicators, targets, and results for each principle, emphasizing how the company manages its sustainability impacts.
Independent Assurance Statement, -Provides details on the assurance engagement (scope, methodology, and limitations) conducted by an external auditor (Bureau Veritas) to verify the sustainability disclosures.

Sustainability Report generally contains:
-Environmental Leadership, -Strengthening Our Value Chain, -Our Approach to Sustainability, -Creating Shared Value for Our People, -Empowered Leadership and Transparent Governance, -Performance Summary 2023-24,-Driving Progress Through Renewable Energy, -Translating Green Ambitions into Reality, -Revolutionizing the Green Fuels Landscape, -Letter from the Chairman, -Strengthening India's Energy Independence, -Embracing Energy Transition, -Fueling Sustainability with Innovation and Technology, -Championing Environmental Sustainability, -Women Empowerment, -Empowering the Society, -Building a Greener Tomorrow with Sustainable Practices, -Supply Chain, -Awards and Recognitions, -Governance, -Policies, Principles, and Practices, -Board of Directors, -Internal Control Systems and Mechanisms, -Risk and Opportunities, -Empowering Digital Transformation, -Memberships, Affiliations, Collaboration, and Advocacy, -Shaping the Future Responsibly, -Future Readiness, -Stakeholder Engagement, -Matters Critical to Value Creation, -Strategic Environment Management for a Sustainable Future, -Energy, -GHG Emissions & Air Quality, -Biodiversity, -Research & Development, -Water Management, -Waste Management, -Sustainability at Residential Complex, -Manpower and Work Environment, -People Management, -Performance Management, Career Growth, and Progression
-Nurturing Talent, -Employee Engagement, -Capacity Building through Training and Development
-Industrial Harmony, -Diversity and Equal Opportunity, -Health and Safety, -Safety Management, -Safety in Transportation, -Advancing Health and Well-being, -Safety and Security of Critical Assets, -Building Lasting Relationships, -Customer Satisfaction, -Quality Management Systems, -Customer-centric Initiatives, -Engaging with the Local Communities, -Fostering Shared Prosperity, -Lives Touched Through CSR Projects, -Economic Performance, -Focusing on Sustainable Returns, -Alignment of Business Practices, -Helping Achieve UN Sustainable Development Goals, -India’s Nationally Determined Contributions (NDCs), -UNGC Principles, -Task Force on Climate-related Financial Disclosures, -Independent Assurance Statement, -GRI Content Index, -List of Abbreviations

Currently there are only following companies available to the worker agent in the Database:
1. Hindustan Petroleum Corporation Limited (HPCL)
2. Indian Oil Corporation Limited (IOCL)

Response Format in json
Return your evaluation strictly in JSON format with the following keys:
"list_of_sub_questions": A python list of sub-questions.

XBRL Datasheets may contain the following KPIs: 
Environmental KPIs like: WhetherDetailsOfGreenHouseGasEmissionsAndItsIntensityIsApplicableToTheCompany, TotalScope1Emissions, TotalScope2Emissions, TotalScope3Emissions, TotalScope1EndScope2EmissionsPerRupeeOfTurnover, TotalWasteGenerated, TotalWasteDisposed, WasteDisposedByLandfilling, WasteDisposedByIncineration, Ewaste, BatteryWaste, PlasticWaste, BioMedicalWaste, RadioactiveWaste, OtherHazardousWaste, ConstructionAndDemolitionWaste, OtherNonHazardousWasteGenerated, WasteIntensityPerRupeeOfTurnover, TotalWasteRecovered, AmountOfReUsed, WasteRecoveredThroughReUsed, AmountOfRecycled, WasteRecoveredThroughRecycled, TotalWaterDischargedInKilolitres, WaterWithdrawalByGroundwater, WaterWithdrawalBySurfaceWater, WaterWithdrawalByThirdPartyWater, TotalVolumeOfWaterConsumption, TotalWaterDischargedInKilolitres, WaterDischargeToGroundwaterWithOutTreatment, WaterDischargeToSurfaceWaterWithTreatment, WaterDischargeToSurfaceWaterWithOutTreatment, WaterDischargeToSeawaterWithTreatment, WaterDischargeToSeawaterWithOutTreatment, WaterDischargeToOthersWithTreatment, WaterDischargeToOthersWithoutTreatment, WasteRecoveredThroughRecycled, WaterIntensityPerRupeeOfTurnover, TotalEnergyConsumedFromRenewableAndNonRenewableSources, TotalEnergyConsumedFromRenewableSources, TotalEnergyConsumedFromNonRenewableSources, EnergyIntensityPerRupeeOfTurnover
Social KPIs like: Training & Awareness, TotalNumberOfTrainingAndAwarenessProgramsHeld, TotalNumberOfEmployeesOrWorkersForTrainingOnHumanRightsIssues, PercentageOfEmployeesOrWorkersCoveredForProvidedTrainingOnHumanRightsIssues, TotalNumberOfEmployeesOrWorkersForTrainingOnHumanRightsIssues, PercentageOfPersonsInRespectiveCategoryCoveredByTheAwarenessProgrammes, PercentageOfPersonsInRespectiveCategoryCoveredByTheAwarenessProgrammes, PercentageOfPersonsInRespectiveCategoryCoveredByTheAwarenessProgrammes, PercentageOfPersonsInRespectiveCategoryCoveredByTheAwarenessProgrammes, PercentageOfPersonsInRespectiveCategoryCoveredByTheAwarenessProgrammes, PercentageOfPersonsInRespectiveCategoryCoveredByTheAwarenessProgrammes, PercentageOfPersonsInRespectiveCategoryCoveredByTheAwarenessProgrammes, PercentageOfPersonsInRespectiveCategoryCoveredByTheAwarenessProgrammes, NumberOfEmployeesOrWhoseFamilyMembersRehabilitatedAndPlacedInSuitableEmployment, NumberOfWorkersOrWhoseFamilyMembersRehabilitatedAndPlacedInSuitableEmployment, PermanentWorkers, TurnoverRate, WhetherAnOccupationalHealthAndSafetyManagementSystemHasBeenImplementedByTheEntity, DetailsOfOccupationalHealthAndSafetyManagementSystemExplanatoryTextBlock, PercentageOfHealthAndSafetyPracticesOfYourPlantsAndOfficesThatWereAssessedP3, TotalRecordableWorkRelatedInjuries, LostTimeInjuryFrequencyRatePerOneMillionPersonHoursWorked, TotalRecordableWorkRelatedInjuries, LostTimeInjuryFrequencyRatePerOneMillionPersonHoursWorked, HighConsequenceWorkRelatedInjuryOrIllHealthExcludingFatalities, HighConsequenceWorkRelatedInjuryOrIllHealthExcludingFatalities , Number of Complaints about Health and Safety pending resolution at year-end, Number of Complaints about working conditions pending resolution at year-end, Number of Complaints about Health and Safety filed by employees and workers during the year, Number of Complaints about working conditions filed by employees and workers during the year, Consumer Protection & Grievances, Number of consumer complaints in respect of the following: - Advertising, - Data privacy, - Cyber-security, - Unfair Trade Practices, - Restrictive Trade Practices, - Delivery of essential services, NumberOfPersonsBenefittedFromCSRProjects, NumberOfEmployeesOrWorkersCoveredForProvidedTrainingOnHumanRightsIssues, PercentageOfChildLabourOfYourPlantsAndOfficesThatWereAssessedP5, TotalComplaintsReportedUnderSexualHarassmentOfWomenAtWorkplace, PercentageOfDiscriminationAtWorkPlaceOfYourPlantsAndOfficesThatWereAssessedP5, PercentageOfDiscriminationAtWorkPlaceOfValueChainPartnersP5, PercentageOfForcedLabourOrInvoluntaryLabourOfValueChainPartnersP5, PercentageOfForcedLabourOrInvoluntaryLabourOfYourPlantsAndOfficesThatWereAssessedP5
Governance KPIs like: PercentageOfInputsWereSourcedSustainably, TotalNumberOfBoardOfDirectorsm, NumberOfFemaleBoardOfDirectors, TotalNumberOfKeyManagementPersonnel, NumberOfFemaleKeyManagementPersonnel, DoesTheEntityHaveAFrameworkOrPolicyOnCyberSecurityAndRisksRelatedToDataPrivacy, WebLinkOfThePolicyOnCyberSecurityAndRisksRelatedToDataPrivacy, NumberOfInstancesOfDataBreachesAlongWithImpact, NumberOfBoardOfDirectorsForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfBoardOfDirectors, NumberOfBoardOfDirectorsForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfBoardOfDirectors, NumberOfBoardOfDirectorsForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfBoardOfDirectors, NumberOfKeyManagerialPersonnelForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfKeyManagerialPersonnel, NumberOfKeyManagerialPersonnelForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfKeyManagerialPersonnel, NumberOfKeyManagerialPersonnelForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfKeyManagerialPersonnel, NumberOfEmployeesOtherThanBodAndKMPForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfEmployeesOtherThanBodAndKMP, NumberOfEmployeesOtherThanBodAndKMPForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfEmployeesOtherThanBodAndKMP, NumberOfEmployeesOtherThanBodAndKMPForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfEmployeesOtherThanBodAndKMP, NumberOfWorkersForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfWorkers, NumberOfWorkersForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfWorkers, NumberOfWorkersForRemunerationOrSalaryOrWages, MedianOfRemunerationOrSalaryOrWagesOfWorkers, totalwages
International Sustainability Reports may contain informations like: Report Title & Subtitle, Approach to Sustainable Development, Purpose and Ambition (e.g., Net Zero Vision), Summary and Introduction, Leadership & Messages, Message from the Chairman/CEO, Message from the Lead Independent Director/Executive Leadership, Governance, Strategy & Oversight, Governance Overview and Board Structure, Committee Structures (e.g., Audit, Compensation, Strategy & CSR), Sustainability Strategy & Roadmap, Materiality, Stakeholder Engagement & Risk Management, Climate-Related and Business Risk Assessment, Climate Change & Energy Transition, Decarbonization Journey / Accelerating Decarbonization, Greenhouse Gas (GHG) Emissions Reduction (Scope 1, 2, & 3), Energy Transition Initiatives & Low-Carbon Energy Strategies, Carbon Pricing and Investment in Energy Efficiency, Climate Impact, Scenario Analysis & Global Challenges, Global Issues (e.g., COP, international climate actions), Operational & Sector-Specific Initiatives, Oil & Gas Operations (Low-emission Assets & Emission Reductions), Gas as a Transition Fuel (Including LNG, Methane Reduction, Geological Storage), Electricity & Renewable Energy (Capacity Build-up, Integrated Power, Electric Mobility), Low-Carbon Fuels and Innovative Energy Solutions (e.g., Sustainable Aviation Fuel, Biofuels), Energy Efficiency Plans & Investment Strategies, Environmental Stewardship, Biodiversity, Water Management & Circular Economy, Environmental Protection and Resource Efficiency, Waste Reduction, Recycling & Circular Management Initiatives, Social Responsibility & Human Capital, People, Communities and Human Rights, Employee Well-being, Health, Safety, and Diversity, Talent Management, Engagement & Inclusion, Community Investment, Social Impact & Local Engagement, Ethical Practices, Transparency & Anti-Corruption, Performance Measurement & Reporting, Key Performance Indicators (KPIs) & Metrics (Emissions, Energy Production/Sales, etc.), Data, Benchmarking & Third-Party Evaluation, Reporting Methodologies, Assurance & Regulatory Disclosures, Taxonomy of Sustainability and Transition Metrics, Future Outlook & Strategic Investments, Near-term Objectives (e.g., 2030 Targets), Long-term Vision (e.g., 2050 Net Zero Ambition), Strategic Investments & Portfolio Resilience, Research, Innovation & Technological Advancements, Just Transition and Social Equity Initiatives, Supplementary & Additional Sections, Case Studies, Regional/Market Focus & “Focus” Sections, Stories and Examples from Sustainability Programs, Appendices, Glossary and Methodological Notes, Integration of Sustainability into Financial & Extra-Financial Performance, Variable Compensation Tied to Sustainability Objectives
"""

manager_system_prompt_O = """
#Role - Role: ESG Specialist with 10 Years of Experience - (Manager Agent)

##Keep in Mind:
-split the give user query into sub-questions, so that the worker agent under you can answer each question.One at a time.
-The worker agent has BRSR and Sustainability Report in the vector searchable Database.

##Response Format in json
Return your evaluation strictly in JSON format with the following keys:
"list_of_sub_questions": A python list of sub-questions.
"""

# Azure AI Search configuration
azure_search_endpoint = os.getenv("AZURE_AI_SEARCH_ENDPOINT")
azure_search_index = os.getenv("AZURE_AI_SEARCH_INDEX")
azure_search_api_key = os.getenv("AZURE_SEARCH_API_KEY")

def manager(
    client,
    deployment,
    user_prompt,
    provided_conversation_history,
    max_iterations,
    connection,
    chat_history_retrieval_limit,
    no_iterations,
    context_chunks,
    agents_conversation_id,
):   
    print("MMMMMMM")
    agents_conversation_id = str(uuid.uuid4())
    print(f"⚪{azure_search_index} AND agents_convertation_id = {agents_conversation_id}")
    agents_conversation_history = agents_conv_history(agents_conversation_id, connection, chat_history_retrieval_limit)
    
    completion = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": manager_system_prompt},
            {"role": "user", "content": f"Previous conversation between user and you: {provided_conversation_history},\nMy question: {user_prompt}"},
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    manager_json_output = completion.choices[0].message.content

    if "```" in manager_json_output:
        manager_json_output = manager_json_output.replace("```json","")
        manager_json_output = manager_json_output.replace("```","")
    
    agent_response = json.loads(manager_json_output) # Parse the JSON response ecplicitly asked
    list_of_sub_questions =agent_response["list_of_sub_questions"]
    print(f"number of sub-questions {len(list_of_sub_questions)}")
    context_chunks = []
    i = 0

    for sub_question in list_of_sub_questions:
        i += 1
        worker_response, context_chunk =  worker(client, deployment, sub_question, agents_conversation_history, azure_search_endpoint, azure_search_index, azure_search_api_key)
        inserting_agent_chat_buffer(agents_conversation_id, connection, sub_question, worker_response, context_chunks)# chuncks used by worker agent

        context_chunks.append(context_chunk)
        print(f"{i}th {sub_question}")

        # if i == max_iterations:
        #     print("THE ITERATION ENDED BECAUSE : Max iterations reached")
        #     break
    no_iterations = i
    direcotr_response = director(
        client,
        deployment,
        user_prompt,
        provided_conversation_history, 
        max_iterations,
        connection,
        chat_history_retrieval_limit,
        no_iterations,
        context_chunks,
        agents_conversation_id,
    )
    return direcotr_response, no_iterations, context_chunks

def director(
    client,
    deployment,
    user_prompt,
    provided_conversation_history,
    max_iterations,
    connection,
    chat_history_retrieval_limit,
    no_iterations,
    context_chunks,
    agents_conversation_id,
):
    print("DDDD")
    agents_conversation_history = agents_conv_history(agents_conversation_id, connection, chat_history_retrieval_limit)
    completion = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": director_system_prompt},
            {"role": "user", "content": f"Previous conversation between user and you: {provided_conversation_history},\nMy question: {user_prompt}"},
            {"role": "assistant", "content": f"Previous conversation between you and worker agent: {agents_conversation_history}"}
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    direcotr_response = completion.choices[0].message.content
    monolog(agents_conversation_history)
    
    # Save the conversation to PDF
    pdf_path = conversation_to_pdf(agents_conversation_history)
    print(f"Conversation saved to PDF: {pdf_path}")
    
    return direcotr_response
