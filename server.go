package raft

type Server struct {
}

func (s *Server) Call(id int, serviceMethod string, args, reply interface{}) error {
	return nil
}
