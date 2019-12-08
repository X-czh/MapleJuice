package membership

func (ml *MembershipList) contains(id string) bool {
	ml.lock.Lock()
	defer ml.lock.Unlock()
	return ml.members.Contains(id)
}

func (ml *MembershipList) add(id string) {
	ml.lock.Lock()
	defer ml.lock.Unlock()
	ml.members.Add(id)
}

func (ml *MembershipList) remove(id string) {
	ml.lock.Lock()
	defer ml.lock.Unlock()
	ml.members.Remove(id)
}

func (ml *MembershipList) getSuccessors(id string, n int) []string {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	if memberLen := ml.members.Size(); memberLen-1 < n {
		n = memberLen - 1
	}
	successors := make([]string, n)

	it := ml.members.Iterator()
	it.Begin()
	it.Next()
	for it.Value() != id {
		it.Next()
	}
	if !it.Next() {
		it.Begin()
		it.Next()
	}
	for counter := 0; counter < n; counter++ {
		if str, ok := it.Value().(string); ok {
			successors[counter] = str
		} else {
			// log.Println("Unrecgonized member id")
		}
		if !it.Next() {
			it.Begin()
			it.Next()
		}
	}

	return successors
}

func (ml *MembershipList) getPredecessors(id string, n int) []string {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	if memberLen := ml.members.Size(); memberLen-1 < n {
		n = memberLen - 1
	}

	predecssors := make([]string, n)

	it := ml.members.Iterator()
	it.Begin()
	it.Next()
	for it.Value() != id {
		it.Next()
	}
	if !it.Prev() {
		it.Last()
	}
	for counter := 0; counter < n; counter++ {
		if str, ok := it.Value().(string); ok {
			predecssors[counter] = str
		} else {
			// log.Println("Unrecognized member id")
		}
		if !it.Prev() {
			it.Last()
		}
	}

	return predecssors
}
