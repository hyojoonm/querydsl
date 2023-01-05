package study.querydsl.entity;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;

@SpringBootTest
@Transactional
public class QuerydslIntermediateTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    // 테스트 실행하기전에 먼저 실행
    @BeforeEach
    public void before(){
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    // 단건조회
    @Test
    public void simpleProjection() throws Exception{
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    // Tuple 은 repository에서만 사용하고 service 나 controller 에 넘기지 말자
    @Test
    public void tupleProjection() throws Exception{
        List<Tuple> result = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            String s = tuple.get(member.username);
            Integer integer = tuple.get(member.age);
            System.out.println("integer = " + integer);
            System.out.println("s = " + s);

        }
    }

    @Test
    public void findDtoByJPQL() throws Exception{
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /*
    결과를 DTO 반환할 때 사용
    다음 3가지 방법 지원
    1. 프로퍼티 접근
    2. 필드 직접 접근
    3. 생성자 사용
     */
    // 세터를 통해서 값을 대입
    @Test
    public void findDtoBySetter() throws Exception{
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // 필드 방식 getter , setter 없어도 필드에 값을 딱 꽂는거라 가능함
    @Test
    public void findDtoByField() throws Exception{
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // 애는 DTO안에 필드값이 딱딱 맞아 떨어져야함
    // constructor 는 실행하는 순간에 오류가 발생함 생성자는 별로
    @Test
    public void findDtoByConstructor() throws Exception{
        List<UserDto> result = queryFactory
                .select(Projections.constructor(UserDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (UserDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // 별칭이 다를 경우 .as 사용하면 딱딱 다 들어간다.
    @Test
    public void findUserDto() throws Exception{
        QMember memberSub = new QMember("memberSub");
        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
                        // 이름이 없을 경우 ExpressionUtils 사용
                        // 10 ,20 ~ 이게 아니라 그냥 전부다 최대 나이로 찍어버림
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                        .from(memberSub),"age")
                ))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }

    /*
    이렇게 하면 DTO를 편하게 사용 가능
    컴파일 오류도 쉽게 해결.
     */
    @Test
    public void findDtoByQueryProjection() throws Exception{
        List<MemberDto> result = queryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // 동적쿼리 Builder 사용
    @Test
    public void dynamicQuery_BooleanBuilder() throws Exception{
        String usernameParam = "member1";
        Integer ageParam = null;

        List<Member> result = saerchMember1(usernameParam,ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> saerchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder();
        if(usernameCond != null){
            builder.and(member.username.eq(usernameCond));
        }

        if(ageCond != null){
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    // 동적 쿼리 - Where 다중 파라미터 사용
    @Test
    public void dynamicQuery_whereParam() throws Exception{
        String usernameParam = "member1";
        Integer ageParam = null;

        List<Member> result = saerchMember2(usernameParam,ageParam);
        assertThat(result.size()).isEqualTo(1);

    }

    private List<Member> saerchMember2(String usernameCond, Integer ageCond) {
        return queryFactory
                .selectFrom(member)
//                .where(usernameEq(usernameCond),ageEq(ageCond))
                .where(allEq(usernameCond,ageCond))
                .fetch();

    }

    private BooleanExpression usernameEq(String usernameCond) {
        if (usernameCond != null) return member.username.eq(usernameCond);
        return null;
    }

    private BooleanExpression ageEq(Integer ageCond) {
        if (ageCond != null) return member.age.eq(ageCond);
        return null;
    }

    // 광고 상태 isValid , 날짜가 IN : isServiceable
//    private BooleanExpression isServiceable(String usernameCond, Integer ageCond){
//        return isValid(usernameCond).and(DataBetweenIn(ageCond));
//    }

    private BooleanExpression allEq(String usernameCond, Integer ageCond){
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    /*
    where 조건에 null 값은 무시된다.
    메서드를 다른 쿼리에서도 재활용 할 수 있다.
    쿼리 자체의 가독성이 높아진다.
    null 체크는 주의해서 처리해야함
     */

    /*
    28살 미만인 사람들은 이름을 비회원으로 바꿈
     */
    @Test
    public void bulkUpdate() throws Exception{

        // member1 = 10 -> 비회원
        // member2 = 20 -> 비회원
        // member3 = 30 -> 유지
        // member4 = 40 -> 유지

        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28)) // lt = 미만
                .execute();

        em.flush(); // 영속성 컨텍스트에 있는 데이터를 DB로 쿼리 전송 (데이터 저장(commit)은 안됨 )
        em.clear(); // 영속성 컨텍스트에 있는 데이터를 제거

        // member1 = 10 -> 1 DB 비회원
        // member2 = 20 -> 2 DB 비회원
        // member3 = 30 -> 3 DB 유지
        // member4 = 40 -> 4 DB 유지
        /*
        영속성 컨텍스트에는 반영이 안되고 DB 에만 반영이 됨
        DB 에서 값을 꺼내와도 영속성 컨테스트에 이미 값이 있어 DB 에서 가져온 값은 무시가 됨
        항상 영속성 컨텍스트가 우선권을 가짐

        벌크 연산을 실행하면 DB랑 영속성 컨텍스트랑 다르기 때문에
        무조건 영속성 컨테스트를 날리고 초기화 하면 됨
         */
        List<Member> result = queryFactory
                .selectFrom(member)
                .fetch();

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }

    }

    /*
    모든 회원 나이 +1
    마이너스는 없으니 add -1
    곱하기는 multiply(2) 2곱하기

     */
    @Test
    public void bulkAdd() throws Exception{
        //given
        long count = queryFactory.update(member)
                .set(member.age, member.age.add(1))
                .execute();
        //when

        //then

    }
    /*
    gt = 이상
    18살 이상 회원 삭제
     */
    @Test
    public void bulkDelete() throws Exception{
        long count = queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }


    // sql function 사용 가능
    @Test
    public void sqlFunction() throws Exception{

        List<String> result = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace',{0},{1} , {2})",
                        member.username, "member", "M"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }

    }

    @Test
    public void sqlFunction2() throws Exception{
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .where(member.username.eq(
                        Expressions.stringTemplate("function('lower',{0})", member.username)))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }
}
