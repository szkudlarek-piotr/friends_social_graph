import os
import pymysql.cursors


def get_interactions_info():
    connection = pymysql.connect(user="SECRET", host="SECRET", passwd="SECRET", database="SECRET")
    with connection.cursor() as cursor:
        cursor.execute("SELECT id, CONCAT(name, ' ', surname) FROM party_people")
        people_dict = dict(cursor.fetchall())
        cursor.execute("SELECT * FROM visit_guest")
        visit_guest_data = cursor.fetchall()
        cursor.execute("SELECT * FROM meeting_human")
        meeting_human_data = cursor.fetchall()
        cursor.execute("SELECT * FROM event_companion")
        event_companion_data = cursor.fetchall()
        cursor.execute("SELECT id, man_id, woman_id FROM weddings")
        marrgiages_info = cursor.fetchall()
        cursor.execute("SELECT * FROM wedding_guest")
        wedding_guests_info = cursor.fetchall()
    return {"people": people_dict, "visits": visit_guest_data, "meetings": meeting_human_data, "events": event_companion_data, "marriages": marrgiages_info, "wed_guests": wedding_guests_info}

def save_edges(crude_data, cutoff, visit_points, meeting_points, event_points, marriage_points, wed_inv_points):
    interactions_dict = {}
    visits_dict = {}
    meetings_dict = {}
    events_dict = {}
    weds_guests_dict = {}
    people_dict = crude_data["people"]

    visit_guest_data = crude_data["visits"]
    for record in visit_guest_data:
        visit_id, human_id = record
        visits_dict.setdefault(visit_id, []).append(human_id)
    for guests_array in visits_dict.values():
        number_of_guests = len(guests_array)
        if number_of_guests > 1:
            interaction_weight = visit_points / (number_of_guests - 1)
            for i in range(0, number_of_guests - 1):
                for j in range(i+1, number_of_guests):
                    pair = tuple(sorted((guests_array[i], guests_array[j])))
                    interactions_dict[pair] = interactions_dict.get(pair, 0) + interaction_weight

    meeting_human_data = crude_data["meetings"]
    for record in meeting_human_data:
        meeting_id, human_id = record
        meetings_dict.setdefault(meeting_id, []).append(human_id)
    for people_array in meetings_dict.values():
        number_of_people = len(people_array)
        if number_of_people > 1:
            interaction_weight = meeting_points / (number_of_people - 1)
            for i in range(0, number_of_people - 1):
                for j in range(i + 1, number_of_people):
                    pair = tuple(sorted((people_array[i], people_array[j])))
                    interactions_dict[pair] = interactions_dict.get(pair, 0) + interaction_weight

    event_companion_data = crude_data["events"]
    for record in event_companion_data:
        event_id, enjoyer_id = record
        events_dict.setdefault(event_id, []).append(enjoyer_id)
    for enjoyers_array in events_dict.values():
        number_of_enjoyers = len(enjoyers_array)
        if number_of_enjoyers > 1:
            interaction_weight = event_points / (number_of_enjoyers - 1)
            for i in range(0, number_of_enjoyers - 1):
                for j in range(i+1, number_of_enjoyers):
                    pair = tuple(sorted((enjoyers_array[i], enjoyers_array[j])))
                    interactions_dict[pair] = interactions_dict.get(pair, 0) + interaction_weight

    marriages = crude_data["marriages"]
    wedding_guests = crude_data["wed_guests"]

    for record in wedding_guests:
        wedding_id, human_id = record
        weds_guests_dict.setdefault(wedding_id, []).append(human_id)

    for marriage in marriages:
        wedding_id = marriage[0]
        groom_id = marriage[1]
        bride_id = marriage[2]
        pair = tuple(sorted((groom_id, bride_id)))
        interactions_dict[pair] = interactions_dict.get(pair, 0) + marriage_points
        if wedding_id in weds_guests_dict:
            for guest_id in weds_guests_dict[wedding_id]:
                pair1 = tuple(sorted((groom_id, guest_id)))
                pair2 = tuple(sorted((bride_id, guest_id)))
                interactions_dict[pair1] = interactions_dict.get(pair1, 0) + wed_inv_points
                interactions_dict[pair2] = interactions_dict.get(pair2, 0) + wed_inv_points

    file_name = f"cutoff_{cutoff}_visits_{visit_points}_meetings_{meeting_points}_events_{event_points}_marriage_{marriage_points}_wedGuest_{wed_inv_points}.csv"
    saving_dir = os.path.join(os.getcwd(), "results", file_name)
    w = open(saving_dir, "w", encoding="utf8")
    w.write("Source,Target,Weight\n")
    #print(interactions_dict)
    for (person1_id, person2_id), edge_weight in interactions_dict.items():
        person1_name = people_dict[person1_id]
        person2_name = people_dict[person2_id]
        weight_to_write = round(edge_weight, 3)
        if weight_to_write > cutoff:
            w.write(f"{person1_name},{person2_name},{weight_to_write}\n")
    w.close()
factor_sets = [[1,1,1,0.5,10,3], [1,1,1.5,0.5,10,3], [1.5,1,1,0.5,10,3], [0.9,1,1,0.5,10,3]]

def scan_factors(factor_sets):
    crude_data = get_interactions_info()
    for factor_set in factor_sets:
        save_edges(crude_data, factor_set[0], factor_set[1], factor_set[2], factor_set[3], factor_set[4], factor_set[5])

scan_factors(factor_sets)