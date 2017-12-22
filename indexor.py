
#Index creation statements of the form
# CREATE INDEX so_l ON SEMANTIC_OBSERVATION(location_id);
# CREATE INDEX so_ult ON SEMANTIC_OBSERVATION(user_id, location_id, timeStamp);

from itertools import chain, combinations


def powerset(iterable):
    s = list(iterable)  # allows duplicate elements
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

attrs = ["user_id", "location_id", "timeStamp", "energy", "temperature", "activity"]


for i, combo in enumerate(powerset(attrs), 1):
	index_name = ""
	for a in combo:
		if(a == 'timeStamp'):
			index_name += 'tS'
			continue;
		index_name += a[0]
	attr_string = ",".join(combo);
	print("CREATE INDEX so_%s ON SEMANTIC_OBSERVATION (%s);" %(index_name, attr_string))				